//! Who may talk to the gateway: where it listens, and the key.
//!
//! The gateway is for the bots of the same machine. By default it listens on a Unix socket in its state directory
//! (Linux and macOS) or on `127.0.0.1` (Windows), and every call carries the access key, a file with mode 600 in the
//! state directory. An address that is not this machine is refused when the gateway starts: it would need TLS, which
//! the gateway does not have yet.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::{Request, Status};
use uuid::Uuid;

/// The metadata header with the key: `authorization: Bearer <key>`.
pub const KEY_HEADER: &str = "authorization";
/// The metadata header with the name of the bot.
pub const BOT_HEADER: &str = "x-brokapi-bot";
/// The name used for a bot that does not give one.
pub const ANONYMOUS: &str = "anonymous";

/// The file with the access key in the state directory.
pub const KEY_FILE: &str = "access.key";
/// The file in the state directory that says where the gateway listens.
pub const ADDRESS_FILE: &str = "gateway.addr";

/// Where the gateway listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Listen {
    /// The default of the platform: the socket `gateway.sock` in the state directory on Linux and macOS, a free port
    /// of `127.0.0.1` on Windows.
    Auto,
    /// A Unix socket at this path (Linux and macOS).
    Unix(PathBuf),
    /// A port of this machine. The address must be a loopback one (`127.0.0.1`, `::1`).
    Tcp(SocketAddr),
}

/// Where a running gateway can be reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    /// A Unix socket.
    Unix(PathBuf),
    /// A port of this machine.
    Tcp(SocketAddr),
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Unix(p) => write!(f, "unix:{}", p.display()),
            Address::Tcp(a) => write!(f, "tcp:{a}"),
        }
    }
}

impl std::str::FromStr for Address {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        if let Some(path) = s.strip_prefix("unix:") {
            Ok(Address::Unix(PathBuf::from(path)))
        } else if let Some(addr) = s.strip_prefix("tcp:") {
            addr.parse()
                .map(Address::Tcp)
                .map_err(|e| format!("{s:?}: {e}"))
        } else {
            Err(format!(
                "{s:?}: an address is unix:<path> or tcp:<host:port>"
            ))
        }
    }
}

/// Whether a bot name is acceptable: 1 to 64 letters, digits, `.`, `_`, `-`.
pub fn valid_bot_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// The key of a gateway, read from its file or made. The file has mode 600; a file that other users can read is refused.
pub fn load_or_create_key(dir: &Path) -> std::io::Result<String> {
    load_or_create_key_in(dir, KEY_FILE)
}

/// [`load_or_create_key`] for a key in a file of another name (the service has its own).
pub(crate) fn load_or_create_key_in(dir: &Path, file: &str) -> std::io::Result<String> {
    let path = dir.join(file);
    if path.exists() {
        check_private(&path)?;
        let key = std::fs::read_to_string(&path)?.trim().to_owned();
        if key.len() >= 32 {
            return Ok(key);
        }
        // An empty or damaged file: make a new key.
    }
    let key = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    write_private(&path, key.as_bytes())?;
    Ok(key)
}

/// The key a client reads from the state directory of the gateway.
pub fn read_key(dir: &Path) -> std::io::Result<String> {
    read_key_in(dir, KEY_FILE)
}

pub(crate) fn read_key_in(dir: &Path, file: &str) -> std::io::Result<String> {
    Ok(std::fs::read_to_string(dir.join(file))?.trim().to_owned())
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    // `mode` applies to a new file only: an old file left with other rights is made private too.
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(unix)]
fn check_private(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} has mode {mode:o}: the access key must be readable by its owner only (chmod 600)",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Compares two keys without stopping at the first difference.
fn same_key(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// The name of the bot that made a call, put in the extensions of the request by [`Access::check`].
#[derive(Debug, Clone)]
pub struct BotName(pub Arc<str>);

/// The check of every call: the key, and the name of the bot.
#[derive(Clone)]
pub struct Access {
    key: Arc<str>,
}

impl Access {
    /// A check for this key.
    pub fn new(key: &str) -> Self {
        Self { key: key.into() }
    }

    /// Passes the call if it carries the key. Puts the [`BotName`] in the request.
    pub fn check(&self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let given = req
            .metadata()
            .get(KEY_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if !same_key(given.as_bytes(), self.key.as_bytes()) {
            return Err(Status::unauthenticated(
                "the access key is missing or wrong: send `authorization: Bearer <contents of access.key>`",
            ));
        }
        let bot = match req.metadata().get(BOT_HEADER) {
            None => ANONYMOUS.to_owned(),
            Some(v) => v
                .to_str()
                .map(str::to_owned)
                .map_err(|_| Status::invalid_argument("the bot name is not text"))?,
        };
        if !valid_bot_name(&bot) {
            return Err(Status::invalid_argument(format!(
                "the bot name {bot:?} is not valid: 1 to 64 letters, digits, '.', '_' or '-'"
            )));
        }
        req.extensions_mut().insert(BotName(bot.into()));
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_compared_whole() {
        assert!(same_key(b"abc", b"abc"));
        assert!(!same_key(b"abc", b"abd"));
        assert!(!same_key(b"abc", b"ab"));
        assert!(!same_key(b"", b"x"));
    }

    #[test]
    fn bot_names() {
        assert!(valid_bot_name("grid-1.a_b"));
        assert!(!valid_bot_name(""));
        assert!(!valid_bot_name("a b"));
        assert!(!valid_bot_name(&"x".repeat(65)));
    }

    #[test]
    fn addresses_round_trip() {
        for text in ["unix:/tmp/x/gateway.sock", "tcp:127.0.0.1:9000"] {
            let a: Address = text.parse().unwrap();
            assert_eq!(a.to_string(), text);
        }
        assert!("http://x".parse::<Address>().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_key_file_is_private_and_a_readable_one_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let key = load_or_create_key(dir.path()).unwrap();
        assert!(key.len() >= 32);
        let path = dir.path().join(KEY_FILE);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        // The second start reads the same key.
        assert_eq!(load_or_create_key(dir.path()).unwrap(), key);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = load_or_create_key(dir.path()).unwrap_err();
        assert!(err.to_string().contains("chmod 600"), "{err}");
    }
}
