//! Naming the uid a session drops into.
//!
//! `DropTo` is numeric; when a caller also supplies `DropTo::name`, the worker —
//! still privileged, in the parent before the post-fork drop — ensures a
//! matching `/etc/passwd` entry so `whoami`, `id -un`, and a shell's `\u` prompt
//! resolve the uid to a real name. Shiitake stays identity-agnostic: it only
//! materializes the name the caller chose for the uid it is already dropping
//! into, and never runs a caller-supplied command as root.
//!
//! Everything created here is undone by the between-session `reset` ([`cleanup`])
//! so a pooled worker does not accumulate names across the sessions it serves.

#[cfg(unix)]
mod imp {
    use shiitake_worker_api::DropTo;
    use std::io::{self, Write};
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use tracing::warn;

    const PASSWD: &str = "/etc/passwd";
    const SKEL: &str = "/etc/skel";

    /// An account this worker created, remembered so `reset` can remove it.
    struct Created {
        name: String,
        uid: u32,
        /// The home directory we made, if `create_home` was set — removed on cleanup.
        home: Option<PathBuf>,
    }

    static CREATED: Mutex<Vec<Created>> = Mutex::new(Vec::new());

    /// Ensure `d.uid` resolves to `d.name` in `/etc/passwd`, creating the entry
    /// (and optionally its home) when missing. Idempotent and best-effort: a
    /// failure to name the account never blocks the session — the uid simply
    /// stays a bare number. Runs in the parent, before the drop, as root.
    ///
    /// Returns the session's home directory when `create_home` is set, so the
    /// caller can point the child's `HOME` at the same path shiitake created and
    /// wrote into passwd (they must agree). `None` means no home was set up.
    pub fn ensure(d: &DropTo) -> io::Result<Option<PathBuf>> {
        let Some(name) = d.name.as_deref() else {
            return Ok(None);
        };
        // A name with these bytes would corrupt the colon/line-delimited file;
        // refuse rather than write a broken passwd. uid 0 is never nameable here.
        if name.is_empty() || name.contains([':', '\n', ' ', '\t']) || d.uid == 0 {
            warn!(
                uid = d.uid,
                name, "refusing to name this account; leaving the uid bare"
            );
            return Ok(None);
        }

        // The canonical home for this session — `<SHIITAKE_HOME_ROOT>/<name>`
        // (root defaults to `/home`). Returned for `HOME` whether we create the
        // entry now or find it already present, so the env always agrees.
        let home = d.create_home.then(|| home_dir(name));

        let existing = std::fs::read_to_string(PASSWD)?;
        // Idempotent: skip when the uid or the name is already present, whether
        // baked into the image or created by an earlier call on this worker.
        if passwd_has(&existing, name, d.uid) {
            return Ok(home);
        }

        let passwd_home = home
            .as_deref()
            .map_or_else(|| "/".to_string(), |h| h.to_string_lossy().into_owned());
        let entry = passwd_entry(name, d.uid, d.gid, &passwd_home);

        // Append under a lock: one worker serves one session at a time, but the
        // lock also guards the CREATED ledger against a concurrent cleanup.
        let mut ledger = CREATED.lock().unwrap();
        let mut f = std::fs::OpenOptions::new().append(true).open(PASSWD)?;
        f.write_all(entry.as_bytes())?;

        let made_home = match &home {
            Some(h) => match provision_home(h, d.uid, d.gid) {
                Ok(()) => Some(h.clone()),
                Err(e) => {
                    warn!(
                        uid = d.uid,
                        name, "created the passwd entry but not its home: {e}"
                    );
                    None
                }
            },
            None => None,
        };

        ledger.push(Created {
            name: name.to_string(),
            uid: d.uid,
            home: made_home,
        });
        Ok(home)
    }

    /// Root under which a named session's home is created — `<root>/<name>`.
    /// Set with `SHIITAKE_HOME_ROOT` (like `SHIITAKE_PTY_SHELL`); defaults to
    /// `/home`. Read once per process.
    fn home_root() -> &'static str {
        static ROOT: OnceLock<String> = OnceLock::new();
        ROOT.get_or_init(|| match std::env::var("SHIITAKE_HOME_ROOT") {
            Ok(r) if !r.trim().is_empty() => r,
            _ => "/home".to_string(),
        })
    }

    fn home_dir(name: &str) -> PathBuf {
        PathBuf::from(home_root()).join(name)
    }

    /// Remove every account this worker created. Called by the between-session
    /// reset. Best-effort per entry: one bad line never strands the rest.
    pub fn cleanup() {
        let drained: Vec<Created> = {
            let mut ledger = CREATED.lock().unwrap();
            std::mem::take(&mut *ledger)
        };
        if drained.is_empty() {
            return;
        }
        if let Err(e) = remove_passwd_lines(&drained) {
            warn!("failed to prune created accounts from {PASSWD}: {e}");
        }
        for acct in &drained {
            let Some(home) = &acct.home else { continue };
            match std::fs::remove_dir_all(home) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => warn!(uid = acct.uid, ?home, "failed to remove session home: {e}"),
            }
        }
    }

    /// Rewrite `/etc/passwd` without the lines we appended, matched on the exact
    /// `name:x:uid:` prefix so we only ever drop our own entries.
    fn remove_passwd_lines(created: &[Created]) -> io::Result<()> {
        let content = std::fs::read_to_string(PASSWD)?;
        let pairs: Vec<(&str, u32)> = created.iter().map(|c| (c.name.as_str(), c.uid)).collect();
        // Truncating write: safe because one worker serves one session at a time,
        // so nothing else is appending concurrently.
        std::fs::write(PASSWD, passwd_without(&content, &pairs))
    }

    /// The passwd line for a named uid. One place so the append format and the
    /// `name:x:uid:` cleanup prefix cannot drift apart.
    pub(super) fn passwd_entry(name: &str, uid: u32, gid: u32, home: &str) -> String {
        format!("{name}:x:{uid}:{gid}:shiitake session:{home}:/bin/bash\n")
    }

    /// True when `content` already has an entry for this name or uid.
    pub(super) fn passwd_has(content: &str, name: &str, uid: u32) -> bool {
        let uid_str = uid.to_string();
        content.lines().any(|line| {
            let mut fields = line.split(':');
            let entry_name = fields.next();
            let entry_uid = fields.nth(1); // skip the password field
            entry_name == Some(name) || entry_uid == Some(uid_str.as_str())
        })
    }

    /// `content` without the `name:x:uid:` lines, preserving the original
    /// trailing-newline shape.
    pub(super) fn passwd_without(content: &str, remove: &[(&str, u32)]) -> String {
        let prefixes: Vec<String> = remove.iter().map(|(n, u)| format!("{n}:x:{u}:")).collect();
        let mut kept = String::with_capacity(content.len());
        for line in content.lines() {
            if prefixes.iter().any(|p| line.starts_with(p)) {
                continue;
            }
            kept.push_str(line);
            kept.push('\n');
        }
        if !content.ends_with('\n') {
            kept.pop();
        }
        kept
    }

    /// Create `home`, seed it from `/etc/skel` if present, and hand the whole
    /// tree to `uid:gid`.
    fn provision_home(home: &std::path::Path, uid: u32, gid: u32) -> io::Result<()> {
        std::fs::create_dir_all(home)?;
        if std::path::Path::new(SKEL).is_dir() {
            copy_tree(std::path::Path::new(SKEL), home)?;
        }
        chown_tree(home, uid, gid)?;
        std::fs::set_permissions(home, std::os::unix::fs::PermissionsExt::from_mode(0o750))?;
        Ok(())
    }

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let dst = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                std::fs::create_dir_all(&dst)?;
                copy_tree(&entry.path(), &dst)?;
            } else {
                std::fs::copy(entry.path(), &dst)?;
            }
        }
        Ok(())
    }

    fn chown_tree(path: &std::path::Path, uid: u32, gid: u32) -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // lchown so a symlink from skel is retargeted, not its referent.
        if unsafe { libc::lchown(c_path.as_ptr(), uid, gid) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // Recurse into real directories only (never follow a symlink out of home).
        if path.is_dir() && !path.symlink_metadata()?.file_type().is_symlink() {
            for entry in std::fs::read_dir(path)? {
                chown_tree(&entry?.path(), uid, gid)?;
            }
        }
        Ok(())
    }
}

#[cfg(all(unix, test))]
mod tests {
    use super::imp::{passwd_entry, passwd_has, passwd_without};

    const BASE: &str =
        "root:x:0:0:root:/root:/bin/bash\ninteractive:x:10009:10009::/:/usr/sbin/nologin\n";

    #[test]
    fn entry_format_and_cleanup_prefix_agree() {
        let line = passwd_entry("alice", 2000123, 10009, "/home/alice");
        assert_eq!(
            line,
            "alice:x:2000123:10009:shiitake session:/home/alice:/bin/bash\n"
        );
        // The line the cleanup prefix `name:x:uid:` must match.
        assert!(line.starts_with("alice:x:2000123:"));
    }

    #[test]
    fn has_matches_by_uid_or_name_not_by_coincidence() {
        assert!(passwd_has(BASE, "root", 999), "existing name is a hit");
        assert!(passwd_has(BASE, "someone", 10009), "existing uid is a hit");
        assert!(
            !passwd_has(BASE, "alice", 2000123),
            "novel name+uid is a miss"
        );
        // A uid that only appears in the gid column must not count as present.
        assert!(!passwd_has(
            "carol:x:1000:10009::/home/carol:/bin/sh\n",
            "alice",
            10009
        ));
    }

    #[test]
    fn without_drops_only_our_exact_entries() {
        let with = format!("{BASE}alice:x:2000123:10009:shiitake session:/home/alice:/bin/bash\n");
        let pruned = passwd_without(&with, &[("alice", 2000123)]);
        assert_eq!(pruned, BASE, "our line goes, the image's users stay");
    }

    #[test]
    fn without_keeps_a_name_reused_at_a_different_uid() {
        // Prefix match is name AND uid, so we never delete an unrelated entry
        // that happens to share the name.
        let content = "alice:x:500:500::/home/alice:/bin/bash\n";
        assert_eq!(passwd_without(content, &[("alice", 2000123)]), content);
    }

    #[test]
    fn without_preserves_missing_trailing_newline() {
        let content = "root:x:0:0:root:/root:/bin/bash"; // no trailing \n
        assert_eq!(passwd_without(content, &[("alice", 1)]), content);
    }
}

#[cfg(not(unix))]
mod imp {
    use shiitake_worker_api::DropTo;
    use std::io;
    use std::path::PathBuf;

    pub fn ensure(_d: &DropTo) -> io::Result<Option<PathBuf>> {
        Ok(None)
    }
    pub fn cleanup() {}
}

pub use imp::{cleanup, ensure};
