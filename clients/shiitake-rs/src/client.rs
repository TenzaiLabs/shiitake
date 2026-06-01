//! Thin async wrapper over `reqwest`: it speaks the `/api/v1` routes and
//! (de)serializes the `shiitake-server-api` types.

use shiitake_server_api::{
    DropTo, ExecRequest, HandleSnapshotJson, HealthResponse, SpawnResponse, StatusResponse,
};
use std::collections::BTreeMap;

/// Which captured stream to read.
#[derive(Debug, Clone, Copy)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn as_str(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("all workers busy (429)")]
    NoIdleWorker,
    #[error("server returned {status}: {body}")]
    Status { status: u16, body: String },
}

/// A slice of a captured stream, read by byte range.
#[derive(Debug, Clone)]
pub struct ReadChunk {
    pub bytes: Vec<u8>,
    /// Total stream size on disk, parsed from `Content-Range` (`bytes a-b/total`
    /// or `bytes */total`) when present.
    pub total: Option<u64>,
}

/// Async client for a single shiitake server.
pub struct Client {
    base: String,
    http: reqwest::Client,
    auth_token: Option<String>,
}

impl Client {
    /// `base_url` is the server origin, e.g. `http://localhost:8080`. The
    /// `/api/v1` prefix is added by the client.
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
            auth_token,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v1{path}", self.base)
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    /// Spawn a command. `command` is run as `bash -c <command>`.
    pub async fn spawn(
        &self,
        command: impl Into<String>,
        workdir: Option<String>,
        timeout: f64,
        env: BTreeMap<String, String>,
        drop_to: Option<DropTo>,
    ) -> Result<SpawnResponse, ClientError> {
        let body = ExecRequest {
            command: command.into(),
            workdir,
            timeout,
            env,
            drop_to,
        };
        let resp = self
            .auth(self.http.post(self.url("/exec")).json(&body))
            .send()
            .await?;
        if resp.status().as_u16() == 429 {
            return Err(ClientError::NoIdleWorker);
        }
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn status(&self, handle: &str) -> Result<StatusResponse, ClientError> {
        let resp = self
            .auth(self.http.get(self.url(&format!("/exec/{handle}"))))
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn kill(&self, handle: &str) -> Result<HandleSnapshotJson, ClientError> {
        let resp = self
            .auth(self.http.delete(self.url(&format!("/exec/{handle}"))))
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        let resp = self.http.get(self.url("/health")).send().await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    /// Read a slice of a captured stream. `range` is an HTTP byte-range spec
    /// such as `bytes=0-` or `bytes=-4096`; `None` fetches the whole stream.
    /// Reading past EOF yields an empty chunk rather than an error.
    pub async fn read(
        &self,
        handle: &str,
        stream: Stream,
        range: Option<&str>,
    ) -> Result<ReadChunk, ClientError> {
        let mut rb = self.auth(
            self.http
                .get(self.url(&format!("/exec/{handle}/{}", stream.as_str()))),
        );
        if let Some(r) = range {
            rb = rb.header(reqwest::header::RANGE, r);
        }
        let resp = rb.send().await?;
        if resp.status().as_u16() == 416 {
            let total = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_total);
            return Ok(ReadChunk {
                bytes: Vec::new(),
                total,
            });
        }
        let resp = error_for_status(resp).await?;
        let total = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_total);
        let bytes = resp.bytes().await?.to_vec();
        Ok(ReadChunk { bytes, total })
    }
}

/// Pull the total size out of a `Content-Range: bytes a-b/total` (or
/// `bytes */total`) header value.
fn parse_total(value: &str) -> Option<u64> {
    value.rsplit('/').next()?.trim().parse().ok()
}

async fn error_for_status(resp: reqwest::Response) -> Result<reqwest::Response, ClientError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let code = status.as_u16();
    let body = resp.text().await.unwrap_or_default();
    Err(ClientError::Status { status: code, body })
}
