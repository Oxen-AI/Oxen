//! Locks in the precedence contract of `liboxen::util::fs::oxen_config_dir`:
//! installed override > `OXEN_CONFIG_DIR` env var > default (home dir).
//!
//! Lives as its own integration test binary so the process-wide `OnceLock` backing
//! `set_oxen_config_dir` starts empty; the unit-test harness in `liboxen::test` calls
//! `set_oxen_config_dir` during `init_test_env`, which would otherwise prevent the
//! env-var branch from being observable.

use std::path::PathBuf;

use liboxen::util;

#[test]
fn env_var_then_override() {
    let env_dir = PathBuf::from("/tmp/oxen-config-precedence-env");
    let override_dir = PathBuf::from("/tmp/oxen-config-precedence-override");

    // SAFETY: this test binary is single-threaded for the duration of the setenv call.
    unsafe {
        std::env::set_var("OXEN_CONFIG_DIR", &env_dir);
    }

    // Branch 1: env var is honored when no override is installed.
    assert_eq!(
        util::fs::oxen_config_dir()
            .expect("oxen_config_dir should resolve from the OXEN_CONFIG_DIR env var"),
        env_dir,
    );

    // Branch 2: installed override wins over the env var.
    util::fs::set_oxen_config_dir(override_dir.clone());
    assert_eq!(
        util::fs::oxen_config_dir()
            .expect("oxen_config_dir should resolve from the installed override"),
        override_dir,
    );
}
