//! `BcsConfig::from_env_file` reads the file it is given, and nothing is searched in the parents.
//! One test only: it changes the environment of the process.

use brokapi_bcs_protocol::config::BcsConfig;

#[test]
fn the_env_file_is_read_from_the_given_path_only() {
    let dir = std::env::temp_dir().join(format!("brokapi-envfile-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("custom.env");
    std::fs::write(
        &file,
        "BCS_REFRESH_TOKEN=from-the-file\nBCS_IS_WRITE=true\n",
    )
    .unwrap();

    // Nothing is set yet and no `.env` is named: no token.
    unsafe {
        std::env::remove_var("BCS_REFRESH_TOKEN");
        std::env::remove_var("BCS_IS_WRITE");
    }
    // A path that does not exist is skipped, then the missing variable is the error.
    assert!(BcsConfig::from_env_file(dir.join("absent.env")).is_err());

    let cfg = BcsConfig::from_env_file(&file).unwrap();
    assert_eq!(cfg.refresh_token, "from-the-file");
    assert!(cfg.is_write);
    // A variable that is set already is not overridden by the file.
    unsafe { std::env::set_var("BCS_REFRESH_TOKEN", "from-the-environment") };
    let cfg = BcsConfig::from_env_file(&file).unwrap();
    assert_eq!(cfg.refresh_token, "from-the-environment");
    let _ = std::fs::remove_dir_all(&dir);
}
