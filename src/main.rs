use anyhow::Error;

use backup_app_rust::backup_opts::BackupOpts;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    tokio::spawn(async move { BackupOpts::process_args().await })
        .await
        .unwrap()
}
