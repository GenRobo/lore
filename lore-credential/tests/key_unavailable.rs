// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! A stored login whose encryption key cannot be read (GRID#3307).
//!
//! One test, in a test binary of its own: the store keeps its path, its key and its token map in
//! process-wide state, which the environment variables set here configure.

use lore_credential::token_store::load_user_token;
use lore_credential::token_store::store_user_token;

const AUTH: &str = "https://auth.example.test:9100";
const USER: &str = "dev";

/// What `lore auth login` leaves behind: a token map with one encrypted token, written by a
/// process whose key this process cannot read (the kernel keyring of a login session that has
/// ended; here, a key file that is not there).
const STORED: &str = r#"[[remotes]]
remote = "https://auth.example.test:9100"

[[remotes.token]]
user_id = "dev"
token = "AQAAANhd3q2Yk0J4bW9yZSB0aGFuIGEgbm9uY2UgYW5kIGEgdGFn"
acceptable_root_domains = []
"#;

#[tokio::test]
async fn a_reader_without_the_key_keeps_the_stored_logins() {
    let dir = std::env::temp_dir().join(format!("lore-credential-key-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // SAFETY: this test binary has one test, so nothing else reads the environment meanwhile.
    unsafe {
        std::env::set_var("LORE_AUTH_PATH", &dir);
        std::env::set_var("LORE_AUTH_STORE", "fallback"); // the key file, never the OS keyring
    }
    let tokens = dir.join("tokens.toml");
    std::fs::write(&tokens, STORED).unwrap();

    // Reading fails, says why, and leaves the logins alone. Before the fix it made a new key
    // and emptied tokens.toml, for this process and for every one that still had the key.
    let error = load_user_token(AUTH, USER, |_| true)
        .await
        .expect_err("a token cannot be read without its key");
    assert!(
        format!("{error} {error:?}").contains("LORE_AUTH_STORE=fallback"),
        "the error says how to recover: {error:?}"
    );
    assert_eq!(std::fs::read_to_string(&tokens).unwrap(), STORED);
    assert!(
        !dir.join("sec-lore_encryption_key").exists(),
        "a read made no new key"
    );

    // A login is the way out: it makes a key, drops what the old key protected, and its token
    // reads back.
    store_user_token(AUTH, USER, "a.new.token", vec![])
        .await
        .unwrap();
    assert_eq!(
        load_user_token(AUTH, USER, |_| true).await.unwrap(),
        "a.new.token"
    );

    std::fs::remove_dir_all(&dir).ok();
}
