use anyhow::Error;

use backup_app_rust::backup_opts::BackupOpts;

#[tokio::main]
async fn main() -> Result<(), Error> {
    BackupOpts::process_args().await
}
