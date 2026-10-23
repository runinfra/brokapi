//! Where a Unix socket can live.
//!
//! The address of a Unix socket is a path, and the system keeps it in a field of a fixed size: 108 bytes on Linux, 104 on
//! macOS and the BSDs, the last one for the closing zero. A state directory in `~/Library/Application Support/...`, a long
//! name of a gateway or a long path after `-c` can make `<state>/gateways/<name>/gateway.sock` longer than that, and the
//! system then refuses to bind (`path must be shorter than SUN_LEN`).
//!
//! The socket is therefore put in the state directory only when the path fits. When it does not, it goes in a short
//! directory of the user, `$XDG_RUNTIME_DIR/brokapi-<uid>` or `/tmp/brokapi-<uid>`, under a name made from the path it
//! stands for. Nobody has to know where it is: the program writes the real address in `gateway.addr` / `service.addr`
//! and the registry, and the clients read it from there.

use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

// `geteuid` cannot fail and takes nothing, so it is safe to call. `uid_t` is a 32-bit number on Linux and macOS.
unsafe extern "C" {
    safe fn geteuid() -> u32;
}

/// The user the process works as.
pub(crate) fn current_uid() -> u32 {
    geteuid()
}

/// The longest path of a socket in bytes: the field of the address is 108 bytes on Linux and 104 elsewhere, and the
/// last byte is the closing zero.
pub(crate) const MAX_SOCKET_PATH: usize = if cfg!(any(target_os = "linux", target_os = "android")) {
    107
} else {
    103
};

/// Whether the system takes this path as the address of a socket.
pub(crate) fn fits(path: &Path) -> bool {
    path.as_os_str().len() <= MAX_SOCKET_PATH
}

/// The path of the socket `name` of a gateway or the service whose state directory is `state_dir`: in the state
/// directory when that fits, in a short private directory of the user when it does not.
pub(crate) fn auto_path(state_dir: &Path, name: &str) -> io::Result<PathBuf> {
    let natural = state_dir.join(name);
    if fits(&natural) {
        return Ok(natural);
    }
    let mut bases = Vec::new();
    if let Some(run) = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        bases.push(run);
    }
    bases.push(PathBuf::from("/tmp"));
    short_path(&natural, &bases, current_uid())
}

/// The short path for the socket that would be at `natural`, in the first of `bases` that works.
fn short_path(natural: &Path, bases: &[PathBuf], uid: u32) -> io::Result<PathBuf> {
    let full = std::path::absolute(natural).unwrap_or_else(|_| natural.to_owned());
    let stem = natural
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("socket");
    let file = format!(
        "{stem}-{:016x}.sock",
        fnv1a(full.as_os_str().as_encoded_bytes())
    );
    let mut why = Vec::new();
    for base in bases {
        let path = base.join(format!("brokapi-{uid}")).join(&file);
        if !fits(&path) {
            why.push(format!("{} is too long too", path.display()));
            continue;
        }
        match private_dir(&base.join(format!("brokapi-{uid}")), uid) {
            Ok(()) => return Ok(path),
            Err(e) => why.push(e.to_string()),
        }
    }
    Err(io::Error::other(format!(
        "the path {} has {} bytes and a Unix socket path may have {MAX_SOCKET_PATH} at most on this system. \
         A short directory for the socket could not be made: {}. \
         Use a shorter state_dir, or give a short path in `listen` (for example `unix:/tmp/brokapi.sock`)",
        natural.display(),
        natural.as_os_str().len(),
        why.join("; ")
    )))
}

/// A directory only the user can enter: made when it is not there, and when it is there, it must be a real directory
/// (not a link) of this user. Another user could have made `/tmp/brokapi-<uid>` first: then it is not used.
fn private_dir(dir: &Path, uid: u32) -> io::Result<()> {
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(io::Error::new(e.kind(), format!("{}: {e}", dir.display()))),
    }
    // `symlink_metadata`: a link put in the place of the directory is a link, not what it points to.
    let meta = std::fs::symlink_metadata(dir)?;
    if !meta.file_type().is_dir() {
        return Err(io::Error::other(format!(
            "{} is not a directory (a link or a file stands there)",
            dir.display()
        )));
    }
    if meta.uid() != uid {
        return Err(io::Error::other(format!(
            "{} belongs to another user",
            dir.display()
        )));
    }
    if meta.mode() & 0o777 != 0o700 {
        // Made by this user with another umask, or opened up by hand: closed again.
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Looks at the place of a socket that is about to be made: `true` when a socket of this user stands there (a gateway that
/// lives, or the trace of one that died), `false` when the place is free. A file that is not a socket, or a socket of
/// another user, is never taken for the trace of a dead gateway and removed: that is an error.
pub(crate) fn inspect(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_socket() => Err(io::Error::other(
            "something that is not a socket is there; it is left alone, move it away or choose another path",
        )),
        Ok(meta) if meta.uid() != current_uid() => Err(io::Error::other(
            "a socket of another user is there; it is left alone, choose another path",
        )),
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// A stable 64-bit hash of bytes (FNV-1a): the same path gives the same name in every version of the program, so a
/// restarted gateway finds the trace of the one that died and takes its place.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, b| {
        (hash ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long_dir(root: &Path) -> PathBuf {
        root.join("a".repeat(60)).join("b".repeat(60))
    }

    #[test]
    fn a_path_that_fits_stays_in_the_state_directory() {
        let dir = Path::new("/tmp/x/state/gateways/main");
        assert_eq!(
            auto_path(dir, "gateway.sock").unwrap(),
            dir.join("gateway.sock")
        );
        // The limit is the field of the system less the closing zero.
        let edge = "x".repeat(MAX_SOCKET_PATH);
        assert!(fits(Path::new(&format!("/{}", &edge[1..]))));
        assert!(!fits(Path::new(&format!("/{edge}"))));
    }

    #[test]
    fn a_long_path_goes_to_a_short_private_directory_under_a_stable_name() {
        let base = tempfile::tempdir().unwrap();
        let natural = long_dir(Path::new("/var/state")).join("gateway.sock");
        let bases = vec![base.path().to_owned()];
        let first = short_path(&natural, &bases, current_uid()).unwrap();
        assert!(fits(&first), "{first:?}");
        assert!(first.starts_with(base.path()));
        let name = first.file_name().unwrap().to_str().unwrap();
        assert!(
            name.starts_with("gateway-") && name.ends_with(".sock"),
            "{name}"
        );
        // The same path gives the same place, another path another one.
        assert_eq!(short_path(&natural, &bases, current_uid()).unwrap(), first);
        let other = long_dir(Path::new("/var/state2")).join("gateway.sock");
        assert_ne!(short_path(&other, &bases, current_uid()).unwrap(), first);
        // The directory is the user's and closed to others.
        let meta = std::fs::metadata(first.parent().unwrap()).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        assert_eq!(meta.uid(), current_uid());
    }

    #[test]
    fn an_open_directory_of_the_user_is_closed_and_a_link_or_a_stranger_is_refused() {
        let base = tempfile::tempdir().unwrap();
        let natural = long_dir(Path::new("/var/state")).join("gateway.sock");
        let bases = vec![base.path().to_owned()];
        let uid = current_uid();
        let dir = base.path().join(format!("brokapi-{uid}"));

        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        short_path(&natural, &bases, uid).unwrap();
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );

        // A link in its place, even to a directory of the user: not used.
        std::fs::remove_dir(&dir).unwrap();
        let elsewhere = base.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &dir).unwrap();
        let refused = short_path(&natural, &bases, uid).unwrap_err().to_string();
        assert!(refused.contains("not a directory"), "{refused}");
        assert!(refused.contains("Unix socket path"), "{refused}");
        std::fs::remove_file(&dir).unwrap();

        // A directory of another user (the uid is not ours): not used.
        std::fs::create_dir(&dir).unwrap();
        let refused = short_path(&natural, &bases, uid + 1)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("another user"), "{refused}");
    }

    #[test]
    fn the_next_base_is_tried_when_the_first_does_not_work() {
        let good = tempfile::tempdir().unwrap();
        let natural = long_dir(Path::new("/var/state")).join("gateway.sock");
        let missing = good.path().join("no/such/place");
        let bases = vec![missing, good.path().to_owned()];
        let path = short_path(&natural, &bases, current_uid()).unwrap();
        assert!(path.starts_with(good.path()));
    }

    #[test]
    fn a_file_in_the_place_of_a_socket_is_not_taken_for_a_dead_gateway() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("s.sock");
        assert!(matches!(inspect(&file), Ok(false)));
        std::fs::write(&file, "mine").unwrap();
        let refused = inspect(&file).err().expect("a file is refused").to_string();
        assert!(refused.contains("not a socket"), "{refused}");
        std::fs::remove_file(&file).unwrap();
        let _listener = std::os::unix::net::UnixListener::bind(&file).unwrap();
        assert!(inspect(&file).unwrap());
    }
}
