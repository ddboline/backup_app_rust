use anyhow::{format_err, Error};
use derive_more::Display;
use flate2::{read::GzDecoder, Compression, GzBuilder};
use futures::future::try_join_all;
use log::{debug, error};
use stack_string::StackString;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
};
use structopt::StructOpt;
use tempfile::NamedTempFile;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc::{
        channel,
        error::{TryRecvError, TrySendError},
        Receiver, Sender,
    },
    task::{spawn, spawn_blocking, JoinHandle},
};
use url::Url;

use crate::{
    config::{Config, Entry},
    s3_instance::S3Instance,
};

#[derive(StructOpt)]
pub struct BackupOpts {
    /// list, backup, restore
    pub command: BackupCommand,
    #[structopt(short = "f", long)]
    pub config_file: PathBuf,
    #[structopt(short, long)]
    pub key: Option<StackString>,
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
            BackupCommand::List => match opts.key {
                None => println!("{}", config),
                Some(key) => {
                    for (k, v) in config.iter() {
                        if &key == k {
                            println!("{} {}", k, v);
                        }
                    }
                }
            },
            BackupCommand::Backup => {
                #[allow(clippy::filter_map)]
                let futures = config
                    .iter()
                    .filter(|(k, _)| match &opts.key {
                        None => true,
                        Some(key) => &key == k,
                    })
                    .map(|(key, val)| async move {
                        match val {
                            Entry::FullPostgresBackup { destination } => {
                                assert!(destination.scheme() == "file");
                                let destination_path: PathBuf = destination.path().into();
                                run_pg_dumpall(destination_path).await?;
                                println!("Finished full_postgres_backup {}", key);
                            }
                            Entry::Postgres {
                                database_url,
                                destination,
                                tables,
                                ..
                            } => {
                                let futures = tables.iter().map(|table| async move {
                                    backup_table(&database_url, &destination, &table).await?;
                                    Ok(())
                                });
                                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                                results?;
                                println!("Finished postgres_backup {}", key);
                            }
                            Entry::Local {
                                require_sudo,
                                destination,
                                backup_paths,
                                command_output,
                                exclude,
                            } => {
                                run_local_backup(
                                    *require_sudo,
                                    destination,
                                    backup_paths,
                                    command_output,
                                    exclude,
                                )
                                .await?;
                                println!("Finished local {}", key);
                            }
                        }
                        Ok(())
                    });
                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                results?;
            }
            BackupCommand::Restore => {
                #[allow(clippy::filter_map)]
                let futures = config
                    .iter()
                    .filter(|(k, _)| match &opts.key {
                        None => true,
                        Some(key) => &key == k,
                    })
                    .map(|(key, val)| async move {
                        match val {
                            Entry::FullPostgresBackup { destination } => {
                                assert!(destination.scheme() == "file");
                                let destination_path: PathBuf = destination.path().into();
                                run_pg_restore(destination_path).await?;
                                println!("Finished restore full_postgres_backup {}", key);
                            }
                            Entry::Postgres {
                                database_url,
                                destination,
                                tables,
                                sequences,
                            } => {
                                let futures = tables.iter().map(|table| async move {
                                    restore_table(&database_url, &destination, &table).await?;
                                    Ok(())
                                });
                                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                                results?;
                                restore_sequences(&database_url, sequences).await?;
                                println!("Finished postgres_retore {}", key);
                            }
                            Entry::Local {
                                require_sudo,
                                destination,
                                ..
                            } => {
                                run_local_restore(*require_sudo, destination).await?;
                                println!("Finished local_restore {}", key);
                            }
                        }
                        Ok(())
                    });
                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                results?;
            }
        }
        Ok(())
    }
}

async fn run_local_backup(
    require_sudo: bool,
    destination: &Url,
    backup_paths: &[PathBuf],
    command_output: &[(StackString, StackString)],
    exclude: &[StackString],
) -> Result<(), Error> {
    let destination = destination.path();
    let mut args = Vec::new();
    if require_sudo {
        args.push("sudo".to_string())
    }
    args.extend_from_slice(&[
        "tar".to_string(),
        "zcvf".to_string(),
        destination.to_string(),
    ]);
    let backup_paths: Vec<String> = backup_paths
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    args.extend_from_slice(&backup_paths);
    for (cmd, output_filename) in command_output {
        let output_args: Vec<_> = cmd.split_whitespace().collect();
        let output = Command::new(output_args[0])
            .args(&output_args[1..])
            .output()
            .await?;
        let mut output_file = File::create(&output_filename).await?;
        output_file.write_all(&output.stdout).await?;
        args.push(output_filename.to_string());
    }
    if !exclude.is_empty() {
        for ex in exclude {
            args.push(format!("--exclude={}", ex));
        }
    }
    let mut p = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;

    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });
    let reader = BufReader::new(stdout);
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_debug(reader, b'\n').await });

    p.await?;
    stdout_task.await??;
    stderr_task.await??;
    Ok(())
}

async fn run_local_restore(require_sudo: bool, destination: &Url) -> Result<(), Error> {
    let destination = destination.path();
    let mut args = Vec::new();
    if require_sudo {
        args.push("sudo".to_string())
    }
    args.extend_from_slice(&[
        "tar".to_string(),
        "zxvf".to_string(),
        destination.to_string(),
    ]);

    let mut p = Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;

    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });
    let reader = BufReader::new(stdout);
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_debug(reader, b'\n').await });

    p.await?;
    stdout_task.await??;
    stderr_task.await??;

    Ok(())
}

async fn backup_table(database_url: &Url, destination: &Url, table: &str) -> Result<(), Error> {
    let tempfile = NamedTempFile::new()?;
    let destination_path = if destination.scheme() == "file" {
        Path::new(destination.path()).join(format!("{}.sql.gz", table))
    } else {
        tempfile.path().to_path_buf()
    };

    let mut p = Command::new("psql")
        .args(&[
            database_url.as_str(),
            "-c",
            &format!("COPY {} TO STDOUT", table),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;
    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_gz_file(stdout, &destination_path).await });
    p.await?;
    stdout_task.await??;
    stderr_task.await??;

    if destination.scheme() == "s3" {
        let s3 = S3Instance::default();
        let bucket = destination
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let key = format!("{}.sql.gz", table);
        s3.upload(tempfile.path(), bucket, &key).await?;
    }
    println!("Finished {}", table);
    Ok(())
}

async fn restore_sequences(
    database_url: &Url,
    sequences: &HashMap<StackString, (StackString, StackString)>,
) -> Result<(), Error> {
    for (seq, (table, id)) in sequences {
        let output = Command::new("psql")
            .args(&[
                database_url.as_str(),
                "-c",
                &format!(
                    "SELECT setval('{}', (SELECT max({}) FROM {}), TRUE)",
                    seq, id, table
                ),
            ])
            .output()
            .await?;
        if !output.stdout.is_empty() {
            debug!("{:?}", output.stdout);
        }
        if !output.stderr.is_empty() {
            error!("{:?}", output.stderr);
        }
    }

    Ok(())
}

async fn restore_table(database_url: &Url, destination: &Url, table: &str) -> Result<(), Error> {
    let tempdir = tempfile::tempdir()?;
    let destination_path = if destination.scheme() == "file" {
        Path::new(destination.path()).join(format!("{}.sql.gz", table))
    } else {
        let s3 = S3Instance::default();
        let bucket = destination
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let key = format!("{}.sql.gz", table);
        let tempfile = tempdir.path().join(&key);
        let fname = tempfile.to_string_lossy();
        s3.download(bucket, &key, fname.as_ref()).await?;
        tempfile
    };

    let output = Command::new("psql")
        .args(&[
            database_url.as_str(),
            "-c",
            &format!("DELETE FROM {}", table),
        ])
        .output()
        .await?;
    if !output.stdout.is_empty() {
        debug!("delete from {}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        error!("delete from {}", String::from_utf8_lossy(&output.stderr));
    }

    let mut p = Command::new("psql")
        .args(&[
            database_url.as_str(),
            "-c",
            &format!("COPY {} FROM STDIN", table),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped())
        .spawn()?;

    let stdin = p.stdin.take().ok_or_else(|| format_err!("No Stdin"))?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;

    let stdin_task: JoinHandle<Result<(), Error>> =
        spawn(async move { input_from_gz_file(stdin, destination_path).await });
    let reader = BufReader::new(stdout);
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_debug(reader, b'\n').await });
    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });

    p.await?;
    stdin_task.await??;
    stdout_task.await??;
    stderr_task.await??;

    Ok(())
}

async fn run_pg_dumpall(destination_path: PathBuf) -> Result<(), Error> {
    let mut p = Command::new("sudo")
        .args(&["-u", "postgres", "/usr/bin/pg_dumpall"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;
    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_gz_file(stdout, &destination_path).await });
    p.await?;
    stdout_task.await??;
    stderr_task.await??;
    Ok(())
}

async fn run_pg_restore(destination_path: PathBuf) -> Result<(), Error> {
    let mut p = Command::new("sudo")
        .args(&["-u", "postgres", "/usr/bin/pg_restore"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdin = p.stdin.take().ok_or_else(|| format_err!("No Stdin"))?;
    let stdout = p.stdout.take().ok_or_else(|| format_err!("No Stdout"))?;
    let stderr = p.stderr.take().ok_or_else(|| format_err!("No Stderr"))?;

    let stdin_task: JoinHandle<Result<(), Error>> =
        spawn(async move { input_from_gz_file(stdin, destination_path).await });
    let reader = BufReader::new(stdout);
    let stdout_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_debug(reader, b'\n').await });
    let reader = BufReader::new(stderr);
    let stderr_task: JoinHandle<Result<(), Error>> =
        spawn(async move { output_to_error(reader, b'\n').await });

    stdin_task.await??;
    p.await?;
    stdout_task.await??;
    stderr_task.await??;

    Ok(())
}

async fn output_to_debug(
    mut reader: BufReader<impl AsyncRead + Unpin>,
    eol: u8,
) -> Result<(), Error> {
    let mut buf = Vec::new();
    while let Ok(bytes) = reader.read_until(eol, &mut buf).await {
        if bytes > 0 {
            debug!("{}", String::from_utf8_lossy(&buf));
        } else {
            break;
        }
        buf.clear();
    }
    Ok(())
}

async fn output_to_error(
    mut reader: BufReader<impl AsyncRead + Unpin>,
    eol: u8,
) -> Result<(), Error> {
    let mut buf = Vec::new();
    while let Ok(bytes) = reader.read_until(eol, &mut buf).await {
        if bytes > 0 {
            error!("{}", String::from_utf8_lossy(&buf));
        } else {
            break;
        }
        buf.clear();
    }
    Ok(())
}

async fn input_from_gz_file(
    mut writer: impl AsyncWriteExt + Unpin,
    input_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let (send, mut recv) = channel(1);
    let input_path = input_path.as_ref().to_path_buf();
    let gz_task = spawn_blocking(move || read_from_gzip(&input_path, send));

    while let Some(buf) = recv.recv().await {
        writer.write_all(&buf).await?;
    }

    gz_task.await??;
    Ok(())
}

fn read_from_gzip(input_path: &Path, mut send: Sender<Vec<u8>>) -> Result<(), Error> {
    use std::{
        fs::File,
        io::{ErrorKind, Read},
    };

    let input_file = File::open(&input_path)?;
    let mut gz = GzDecoder::new(input_file);

    loop {
        let mut buf = vec![0u8; 4096];
        let mut offset = 0;
        loop {
            match gz.read(&mut buf[offset..]) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    offset += n;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        buf.truncate(offset);
        let mut buf_opt = Some(buf);
        while let Some(buf) = buf_opt.take() {
            match send.try_send(buf) {
                Ok(_) => {
                    break;
                }
                Err(TrySendError::Full(buf)) => {
                    buf_opt.replace(buf);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        if offset < 4096 {
            break;
        }
    }
    Ok(())
}

async fn output_to_gz_file(
    mut reader: impl AsyncReadExt + Unpin,
    output_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let (mut send, recv) = channel(1);
    let output_path = output_path.as_ref().to_path_buf();
    let gz_task = spawn_blocking(move || write_to_gzip(&output_path, recv));

    loop {
        let mut buf = Vec::with_capacity(4096);
        let bytes = reader.read_buf(&mut buf).await?;
        if bytes == 0 {
            break;
        }
        send.send(buf).await?;
    }
    drop(send);

    gz_task.await??;

    Ok(())
}

fn write_to_gzip(output_path: &Path, mut recv: Receiver<Vec<u8>>) -> Result<(), Error> {
    use std::{fs::File, io::Write, thread::sleep, time::Duration};

    let file_name = output_path
        .file_name()
        .ok_or_else(|| format_err!("No file name"))?
        .to_string_lossy()
        .into_owned();
    let output_file = File::create(&output_path)?;
    let mut gz = GzBuilder::new()
        .filename(file_name)
        .write(output_file, Compression::default());
    loop {
        match recv.try_recv() {
            Ok(buf) => {
                gz.write_all(&buf)?;
            }
            Err(TryRecvError::Closed) => {
                break;
            }
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(100));
            }
        }
    }
    gz.try_finish()?;
    Ok(())
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
