//! Thin async wrapper over `reqwest`: it speaks the `/api/v1` routes and
//! (de)serializes the `shiitake-server-api` types.

use shiitake_server_api::{
    CreateSessionRequest, DropTo, HandleSnapshotJson, HealthResponse, ProfileResponse,
    ReadyResponse, SessionCommandMode, SessionCommandRequest, SessionId, SessionResponse,
    SpawnResponse, StatusResponse,
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

/// A worker reserved in one profile. Call `release` when finished.
pub struct Session<'a> {
    client: &'a Client,
    pub details: SessionResponse,
}

impl Session<'_> {
    pub async fn submit(
        &self,
        command: impl Into<String>,
        timeout: f64,
        mode: SessionCommandMode,
    ) -> Result<SpawnResponse, ClientError> {
        self.client
            .submit(&self.details.session_id, command, timeout, mode)
            .await
    }

    pub async fn status(&self, handle: &str) -> Result<StatusResponse, ClientError> {
        self.client
            .command_status(&self.details.session_id, handle)
            .await
    }

    pub async fn kill(&self, handle: &str) -> Result<HandleSnapshotJson, ClientError> {
        self.client
            .kill_command(&self.details.session_id, handle)
            .await
    }

    pub async fn read(
        &self,
        handle: &str,
        stream: Stream,
        range: Option<&str>,
    ) -> Result<ReadChunk, ClientError> {
        self.client
            .read_command(&self.details.session_id, handle, stream, range)
            .await
    }

    pub async fn release(self) -> Result<SessionResponse, ClientError> {
        self.client.release_session(&self.details.session_id).await
    }
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

    pub async fn profiles(&self) -> Result<Vec<ProfileResponse>, ClientError> {
        let resp = self
            .auth(self.http.get(self.url("/profiles")))
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn create_session(
        &self,
        profile: impl Into<String>,
        workdir: Option<String>,
        env: BTreeMap<String, String>,
        drop_to: Option<DropTo>,
    ) -> Result<Session<'_>, ClientError> {
        let body = CreateSessionRequest {
            profile: profile.into(),
            workdir,
            env,
            drop_to,
            cols: 80,
            rows: 24,
        };
        let resp = self
            .auth(self.http.post(self.url("/sessions")).json(&body))
            .send()
            .await?;
        if resp.status().as_u16() == 429 {
            return Err(ClientError::NoIdleWorker);
        }
        let details = error_for_status(resp).await?.json().await?;
        Ok(Session {
            client: self,
            details,
        })
    }

    pub async fn session_status(&self, id: &SessionId) -> Result<SessionResponse, ClientError> {
        let resp = self
            .auth(self.http.get(self.url(&format!("/sessions/{id}"))))
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn release_session(&self, id: &SessionId) -> Result<SessionResponse, ClientError> {
        let resp = self
            .auth(self.http.delete(self.url(&format!("/sessions/{id}"))))
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn submit(
        &self,
        id: &SessionId,
        command: impl Into<String>,
        timeout: f64,
        mode: SessionCommandMode,
    ) -> Result<SpawnResponse, ClientError> {
        let body = SessionCommandRequest {
            command: command.into(),
            timeout,
            mode,
        };
        let resp = self
            .auth(
                self.http
                    .post(self.url(&format!("/sessions/{id}/commands")))
                    .json(&body),
            )
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn command_status(
        &self,
        id: &SessionId,
        handle: &str,
    ) -> Result<StatusResponse, ClientError> {
        let resp = self
            .auth(
                self.http
                    .get(self.url(&format!("/sessions/{id}/commands/{handle}"))),
            )
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn kill_command(
        &self,
        id: &SessionId,
        handle: &str,
    ) -> Result<HandleSnapshotJson, ClientError> {
        let resp = self
            .auth(
                self.http
                    .delete(self.url(&format!("/sessions/{id}/commands/{handle}"))),
            )
            .send()
            .await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    pub async fn read_command(
        &self,
        id: &SessionId,
        handle: &str,
        stream: Stream,
        range: Option<&str>,
    ) -> Result<ReadChunk, ClientError> {
        let mut rb = self.auth(self.http.get(self.url(&format!(
            "/sessions/{id}/commands/{handle}/{}",
            stream.as_str()
        ))));
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
        Ok(ReadChunk {
            bytes: resp.bytes().await?.to_vec(),
            total,
        })
    }

    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        let resp = self.http.get(self.url("/health")).send().await?;
        Ok(error_for_status(resp).await?.json().await?)
    }

    /// Readiness: whether the pool has enough registered workers to serve.
    /// The server answers `503` when it is not ready, which is a valid answer
    /// rather than a transport failure — the verdict is returned on
    /// [`ReadyResponse::ready`] either way.
    pub async fn ready(&self) -> Result<ReadyResponse, ClientError> {
        let resp = self.http.get(self.url("/ready")).send().await?;
        if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Ok(resp.json().await?);
        }
        Ok(error_for_status(resp).await?.json().await?)
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
