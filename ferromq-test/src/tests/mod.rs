//! Test suites

use std::path::PathBuf;

pub mod chaos;
pub mod functional;
pub mod stress;

/// Resolve a test broker config path, e.g. `config_path("retain-disabled")`
/// -> `<workspace>/ferromq-test/configs/retain-disabled/ferromq.toml`.
///
/// Built from `CARGO_MANIFEST_DIR` (the `ferromq-test` crate dir), so the
/// returned path is absolute and independent of the process working
/// directory. Config files must live under `ferromq-test/configs/<name>/` and
/// be self-contained (their own `plugins/` sub-dir).
pub fn config_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs").join(name).join("ferromq.toml")
}
