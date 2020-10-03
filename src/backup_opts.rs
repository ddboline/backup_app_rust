use anyhow::{format_err, Error};
use derive_more::Display;
use std::path::PathBuf;
use std::str::FromStr;
use structopt::StructOpt;
use tokio::process::Command;
use std::process::Stdio;
use flate2::write::GzEncoder;

use crate::config::{Config, Entry};

#[derive(StructOpt)]
pub struct BackupOpts {
    pub command: BackupCommand,
    #[structopt(short = "f", long)]
    pub config_file: PathBuf,
}

impl BackupOpts {
    pub async fn process_args() -> Result<(), Error> {
        let opts = Self::from_args();
        if !opts.config_file.exists() {
            return Err(format_err!(
                "Config file {:?} does not exist",
                opts.config_file
            ));
        }
        let config = Config::new(&opts.config_file)?;
        match opts.command {
            BackupCommand::List => {
                println!("{}", config);
            }
            BackupCommand::Backup => {
                for (key, val) in config.iter() {
                    match val {
                        Entry::FullPostgresBackup {destination} => {
                            let mut task = Command::new("sudo")
                                .args(&["-u", "postgres", "/usr/bin/pg_dumpall"])
                                .stdout(Stdio::piped())
                                .spawn()?;
                            let stdout = task.stdout.take();

                        },
                        Entry::Postgres {database_url, destination, tables, sequences} => {},
                        Entry::Local {require_sudo, destination, backup_paths, command_output, exclude} => {},
                    }
                }
            }
            BackupCommand::Restore => {}
        }
        Ok(())
    }
}

#[derive(Display)]
pub enum BackupCommand {
    #[display(fmt = "list")]
    List,
    #[display(fmt = "backup")]
    Backup,
    #[display(fmt = "restore")]
    Restore,
}

impl FromStr for BackupCommand {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "list" => Ok(Self::List),
            "backup" => Ok(Self::Backup),
            "restore" => Ok(Self::Restore),
            _ => Err(format_err!("Invalid command")),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;

    use crate::backup_opts::BackupCommand;

    #[test]
    fn test_backupcommand_display() -> Result<(), Error> {
        let s = format!(
            "{} {} {}",
            BackupCommand::List,
            BackupCommand::Backup,
            BackupCommand::Restore
        );
        assert_eq!(&s, "list backup restore");
        Ok(())
    }
}
