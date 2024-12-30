use anyhow::{format_err, Error};
use clap::Parser;
use deadqueue::unlimited::Queue;
use derive_more::{Deref, Display};
use flate2::{read::GzDecoder, Compression, GzBuilder};
use futures::{stream::FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use log::{debug, error};
use stack_string::{format_sstr, StackString};
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
};
use tempfile::NamedTempFile;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{
        mpsc::{channel, error::TrySendError, Receiver, Sender},
        oneshot,
    },
    task::{spawn, JoinHandle},
};
use url::Url;

use crate::{
    config::{Config, Entry},
    s3_instance::S3Instance,
};

fn spawn_threadpool<F, R>(f: F) -> oneshot::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static + std::fmt::Debug,
{
    let (send, recv) = oneshot::channel();
    rayon::spawn(move || {
        if let Err(e) = send.send(f()) {
            error!("Send error {:?}", e);
        }
    });
    recv
}

#[derive(Display, Clone, Copy)]
pub enum BackupCommand {
    #[display("list")]
    List,
    #[display("backup")]
    Backup,
    #[display("restore")]
    Restore,
    #[display("clear")]
    Clear,
}

impl FromStr for BackupCommand {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "list" => Ok(Self::List),
            "backup" => Ok(Self::Backup),
            "restore" => Ok(Self::Restore),
            "clear" => Ok(Self::Clear),
            _ => Err(format_err!("Invalid command")),
        }
    }
}

struct BackupEntry {
    command: BackupCommand,
    key: StackString,
    entry: Entry,
}

#[derive(Clone, Deref)]
struct BackupQueue(Arc<Queue<Option<BackupEntry>>>);

impl BackupQueue {
    fn new() -> Self {
        Self(Arc::new(Queue::new()))
    }
}

#[derive(Parser)]
pub struct BackupOpts {
    /// list, backup, restore, clear
    pub command: BackupCommand,
    #[clap(short = 'f', long)]
    pub config_file: PathBuf,
    #[clap(short, long)]
    pub key: Option<StackString>,
    #[clap(short, long)]
    pub num_workers: Option<usize>,
}

impl BackupOpts {
    /// # Errors
    /// Return error:
    ///     * if config init fails
    ///     * if threadpool build fails
    ///     * if worker tasks return error or panic
    pub async fn process_args() -> Result<(), Error> {
        let opts = Self::parse();
        if !opts.config_file.exists() {
            return Err(format_err!(
                "Config file {} does not exist",
                opts.config_file.to_string_lossy()
            ));
        }
        let queue = BackupQueue::new();
        let config = Config::new(&opts.config_file)?;
        let num_workers = opts.num_workers.unwrap_or_else(num_cpus::get);
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_workers * 2)
            .build_global()?;
        let worker_tasks: Vec<_> = (0..num_workers)
            .map(|_| {
                let queue = queue.clone();
                spawn(async move { worker_task(&queue).await })
            })
            .collect();

        for (key, entry) in config {
            if opts.key.is_none() || opts.key.as_ref() == Some(&key) {
                let entry = BackupEntry {
                    command: opts.command,
                    key,
                    entry,
                };
                queue.push(Some(entry));
            }
        }

        worker_tasks.iter().for_each(|_| queue.push(None));

        for task in worker_tasks {
            task.await??;
        }
        Ok(())
    }
}

async fn worker_task(queue: &Queue<Option<BackupEntry>>) -> Result<(), Error> {
    while let Some(entry) = queue.pop().await {
        process_entry(&entry).await?;
    }
    Ok(())
}

async fn full_backup(destination: &Url) -> Result<(), Error> {
    assert!(destination.scheme() == "file");
    let destination_path: PathBuf = destination.path().into();
    run_pg_dumpall(destination_path).await
}

async fn run_postgres_backup(
    database_url: &Url,
    destination: &Url,
    tables: &[StackString],
    columns: &HashMap<StackString, Vec<StackString>>,
) -> Result<(), Error> {
    let futures: FuturesUnordered<_> = tables
        .iter()
        .map(|table| async move {
            let empty = Vec::new();
            let columns = columns.get(table).unwrap_or(&empty);
            backup_table(database_url, destination, table, columns).await?;
            Ok(())
        })
        .collect();
    futures.try_collect().await
}

async fn run_postgres_restore(
    database_url: &Url,
    destination: &Url,
    tables: &[StackString],
    columns: &HashMap<StackString, Vec<StackString>>,
    dependencies: &HashMap<StackString, Vec<StackString>>,
    sequences: &HashMap<StackString, (StackString, StackString)>,
) -> Result<(), Error> {
    let full_deps = get_full_deps(tables, columns.keys(), dependencies);

    let mut parents_graph = get_parent_graph(&full_deps);
    for t in full_deps.keys() {
        if !parents_graph.contains_key(t.as_str()) {
            parents_graph.entry(t.as_str()).or_default();
        }
    }

    process_tasks(&full_deps, |t| {
        let t: StackString = t.into();
        async move {
            clear_table(database_url, &t).await?;
            Ok(())
        }
    })
    .await?;

    println!("finished clearing");

    process_tasks(&parents_graph, |t| {
        let empty = Vec::new();
        let columns = columns.get(t).unwrap_or(&empty).clone();
        let t: StackString = t.into();
        async move {
            restore_table(database_url, destination, &t, &columns).await?;
            Ok(())
        }
    })
    .await?;

    restore_sequences(database_url, sequences).await?;
    Ok(())
}

async fn process_entry(backup_entry: &BackupEntry) -> Result<(), Error> {
    let key = &backup_entry.key;
    let entry = &backup_entry.entry;
    match backup_entry.command {
        BackupCommand::List => {
            println!("{key} {entry}");
        }
        BackupCommand::Backup => match entry {
            Entry::FullPostgresBackup { destination } => {
                full_backup(destination).await?;
                println!("Finished full_postgres_backup {key}");
            }
            Entry::Postgres {
                database_url,
                destination,
                tables,
                columns,
                ..
            } => {
                run_postgres_backup(database_url, destination, tables, columns).await?;
                println!("Finished postgres_backup {key}");
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
                println!("Finished local {key}");
            }
        },
        BackupCommand::Restore => match entry {
            Entry::FullPostgresBackup { destination } => {
                assert!(destination.scheme() == "file");
                let destination_path: PathBuf = destination.path().into();
                run_pg_restore(destination_path).await?;
                println!("Finished restore full_postgres_backup {key}");
            }
            Entry::Postgres {
                database_url,
                destination,
                tables,
                columns,
                dependencies,
                sequences,
            } => {
                run_postgres_restore(
                    database_url,
                    destination,
                    tables,
                    columns,
                    dependencies,
                    sequences,
                )
                .await?;
                println!("Finished postgres_restore {key}");
            }
            Entry::Local {
                require_sudo,
                destination,
                ..
            } => {
                run_local_restore(*require_sudo, destination).await?;
                println!("Finished local_restore {key}");
            }
        },
        BackupCommand::Clear => {
            if let Entry::Postgres {
                database_url,
                tables,
                columns,
                dependencies,
                ..
            } = entry
            {
                let full_deps = get_full_deps(tables, columns.keys(), dependencies);

                process_tasks(&full_deps, |t| {
                    let t: StackString = t.into();
                    async move {
                        clear_table(database_url, &t).await?;
                        Ok(())
                    }
                })
                .await?;

                println!("finished clearing");
            }
        }
    };
    Ok(())
}

fn get_full_deps(
    tables: impl IntoIterator<Item = impl AsRef<str>>,
    columns: impl Iterator<Item = impl AsRef<str>>,
    dependencies: impl IntoIterator<Item = (impl AsRef<str>, impl IntoIterator<Item = impl AsRef<str>>)>,
) -> HashMap<StackString, BTreeSet<StackString>> {
    let mut full_deps: HashMap<StackString, BTreeSet<StackString>> = HashMap::new();
    for t in tables {
        full_deps.insert(t.as_ref().into(), BTreeSet::new());
    }
    for t in columns {
        full_deps.entry(t.as_ref().into()).or_default();
    }
    for (k, v) in dependencies {
        for child in v {
            full_deps
                .entry(k.as_ref().into())
                .or_default()
                .insert(child.as_ref().into());
            full_deps.entry(child.as_ref().into()).or_default();
        }
    }
    full_deps
}

async fn run_local_backup(
    require_sudo: bool,
    destination: &Url,
    backup_paths: &[impl AsRef<Path>],
    command_output: &[(impl AsRef<str>, impl AsRef<str>)],
    exclude: &[impl AsRef<str>],
) -> Result<(), Error> {
    let user = std::env::var("USER").unwrap_or_else(|_| "root".into());
    let destination = destination.path();
    let mut args: Vec<StackString> = Vec::new();
    if require_sudo {
        args.push("sudo".into());
    }
    args.extend_from_slice(&["tar".into(), "zcvf".into(), destination.into()]);
    if !exclude.is_empty() {
        for ex in exclude {
            args.push(format_sstr!("--exclude={}", ex.as_ref()));
        }
    }
    let backup_paths: Vec<_> = backup_paths
        .iter()
        .map(|p| p.as_ref().to_string_lossy().into_owned().into())
        .collect();
    args.extend_from_slice(&backup_paths);
    for (cmd, output_filename) in command_output {
        let output_args: Vec<_> = cmd.as_ref().split_whitespace().collect();
        let output = Command::new(output_args[0])
            .args(&output_args[1..])
            .output()
            .await?;
        let mut output_file = File::create(output_filename.as_ref()).await?;
        output_file.write_all(&output.stdout).await?;
        args.push(output_filename.as_ref().into());
    }
    let program = &args[0];
    let mut p = Command::new(program)
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

    let status = p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("{program} returned with {status}");
    }

    if require_sudo {
        let output = Command::new("sudo")
            .args(["chown", &format_sstr!("{user}:{user}"), destination])
            .output()
            .await?;
        if !output.stdout.is_empty() {
            debug!("{}", String::from_utf8_lossy(&output.stdout));
        }
        if !output.stderr.is_empty() {
            error!("{}", String::from_utf8_lossy(&output.stderr));
        }
    }
    Ok(())
}

async fn run_local_restore(require_sudo: bool, destination: &Url) -> Result<(), Error> {
    let destination = destination.path();
    let mut args: Vec<StackString> = Vec::new();
    if require_sudo {
        args.push("sudo".into());
    }
    args.extend_from_slice(&["tar".into(), "zxvf".into(), destination.into()]);

    let program = &args[0];
    let mut p = Command::new(program)
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

    let status = p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("{program} returned with {status}");
    }

    Ok(())
}

async fn backup_table(
    database_url: &Url,
    destination: &Url,
    table: &str,
    columns: &[impl AsRef<str>],
) -> Result<(), Error> {
    let tempfile = NamedTempFile::new()?;
    let destination_path = if destination.scheme() == "file" {
        Path::new(destination.path()).join(format_sstr!("{table}.sql.gz"))
    } else {
        tempfile.path().to_path_buf()
    };
    let query = if columns.is_empty() {
        format_sstr!("COPY {table} TO STDOUT")
    } else {
        format_sstr!(
            "COPY {table} ({c}) TO STDOUT",
            c = columns.iter().map(AsRef::as_ref).join(",")
        )
    };

    let mut p = Command::new("psql")
        .args([database_url.as_str(), "-c", &query])
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
    let status = p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("psql returned with {status}");
    }

    if destination.scheme() == "s3" {
        let sdk_config = aws_config::load_from_env().await;
        let s3 = S3Instance::new(&sdk_config);
        let bucket = destination
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let key = format_sstr!("{table}.sql.gz");
        s3.upload(tempfile.path(), bucket, &key).await?;
    }
    Ok(())
}

async fn restore_sequences(
    database_url: &Url,
    sequences: &HashMap<StackString, (StackString, StackString)>,
) -> Result<(), Error> {
    for (seq, (table, id)) in sequences {
        let output = Command::new("psql")
            .args([
                database_url.as_str(),
                "-c",
                &format_sstr!("SELECT setval('{seq}', (SELECT max({id}) FROM {table}), TRUE)"),
            ])
            .output()
            .await?;
        if !output.stdout.is_empty() {
            debug!("{}", String::from_utf8_lossy(&output.stdout));
        }
        if !output.stderr.is_empty() {
            error!("{}", String::from_utf8_lossy(&output.stderr));
        }
    }

    Ok(())
}

async fn clear_table(database_url: &Url, table: &str) -> Result<(), Error> {
    let output = Command::new("psql")
        .args([
            database_url.as_str(),
            "-c",
            &format_sstr!("DELETE FROM {table}"),
        ])
        .output()
        .await?;
    if !output.stdout.is_empty() {
        debug!("delete from {}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        error!("delete from {}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(())
}

async fn restore_table(
    database_url: &Url,
    destination: &Url,
    table: &str,
    columns: &[impl AsRef<str>],
) -> Result<(), Error> {
    let tempdir = tempfile::tempdir()?;
    let destination_path = if destination.scheme() == "file" {
        Path::new(destination.path()).join(format_sstr!("{table}.sql.gz"))
    } else {
        let sdk_config = aws_config::load_from_env().await;
        let s3 = S3Instance::new(&sdk_config);
        let bucket = destination
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let key = format_sstr!("{table}.sql.gz");
        let tempfile = tempdir.path().join(&key);
        let fname = tempfile.to_string_lossy();
        s3.download(bucket, &key, fname.as_ref()).await?;
        tempfile
    };
    let query = if columns.is_empty() {
        format_sstr!("COPY {table} FROM STDIN")
    } else {
        format_sstr!(
            "COPY {table} ({c}) FROM STDIN",
            c = columns.iter().map(AsRef::as_ref).join(",")
        )
    };

    let mut p = Command::new("psql")
        .args([database_url.as_str(), "-c", &query])
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

    let status = p.wait().await?;
    stdin_task.await??;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("psql returned with {status}");
    }

    Ok(())
}

async fn run_pg_dumpall(destination_path: PathBuf) -> Result<(), Error> {
    let mut p = Command::new("sudo")
        .args(["-u", "postgres", "/usr/bin/pg_dumpall"])
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
    let status = p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("pg_dumpall returned with {status}");
    }
    Ok(())
}

async fn run_pg_restore(destination_path: PathBuf) -> Result<(), Error> {
    let mut p = Command::new("sudo")
        .args(["-u", "postgres", "/usr/bin/pg_restore"])
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
    let status = p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if !status.success() {
        println!("pg_restore returned with {status}");
    }

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
    let gz_task = spawn_threadpool(move || read_from_gzip(&input_path, &send));

    while let Some(buf) = recv.recv().await {
        writer.write_all(&buf).await?;
    }

    gz_task.await??;
    Ok(())
}

fn read_from_gzip(input_path: &Path, send: &Sender<Vec<u8>>) -> Result<(), Error> {
    use std::{
        fs::File,
        io::{ErrorKind, Read},
    };

    let input_file = File::open(input_path)?;
    let mut gz = GzDecoder::new(input_file);

    loop {
        let mut buf = vec![0_u8; 4096];
        let mut offset = 0;
        loop {
            match gz.read(&mut buf[offset..]) {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                    offset += n;
                }
                Err(e) => {
                    if e.kind() != ErrorKind::Interrupted {
                        return Err(e.into());
                    }
                }
            }
        }
        buf.truncate(offset);
        let mut buf_opt = Some(buf);
        while let Some(buf) = buf_opt.take() {
            match send.try_send(buf) {
                Ok(()) => {
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
    let (send, recv) = channel(1);
    let output_path = output_path.as_ref().to_path_buf();
    let gz_task = spawn_threadpool(move || write_to_gzip(&output_path, recv));

    loop {
        let mut buf = Vec::with_capacity(4096);
        if reader.read_buf(&mut buf).await? == 0 {
            break;
        }
        send.send(buf).await?;
    }
    drop(send);

    gz_task.await??;

    Ok(())
}

fn write_to_gzip(output_path: &Path, mut recv: Receiver<Vec<u8>>) -> Result<(), Error> {
    use std::{fs::File, io::Write};

    let file_name = output_path
        .file_name()
        .ok_or_else(|| format_err!("No file name"))?
        .to_string_lossy()
        .into_owned();
    let output_file = File::create(output_path)?;
    let mut gz = GzBuilder::new()
        .filename(file_name)
        .write(output_file, Compression::default());
    while let Some(buf) = recv.blocking_recv() {
        gz.write_all(&buf)?;
    }
    gz.try_finish()?;
    Ok(())
}

struct TaskNode<T>
where
    T: Future<Output = Result<(), Error>>,
{
    recvs: Vec<Receiver<StackString>>,
    task: T,
    sends: Vec<Sender<StackString>>,
}

impl<T> TaskNode<T>
where
    T: Future<Output = Result<(), Error>>,
{
    fn new(task: T) -> Self {
        Self {
            recvs: Vec::new(),
            task,
            sends: Vec::new(),
        }
    }
}

async fn process_tasks<F, T>(
    dag: &HashMap<impl AsRef<str>, BTreeSet<impl AsRef<str>>>,
    task: F,
) -> Result<(), Error>
where
    F: Fn(&str) -> T,
    T: Future<Output = Result<(), Error>>,
{
    let mut tasks: HashMap<_, _> = dag
        .keys()
        .map(|element| (element.as_ref(), TaskNode::new(task(element.as_ref()))))
        .collect();
    println!("{:?}", tasks.keys().join(","));
    for (key, val) in dag {
        for child in val {
            let (s, r) = channel(1);
            println!("{} {}", key.as_ref(), child.as_ref());
            if let Some(task) = tasks.get_mut(key.as_ref()) {
                task.sends.push(s);
            }
            if let Some(task) = tasks.get_mut(child.as_ref()) {
                task.recvs.push(r);
            }
        }
    }

    let futures: FuturesUnordered<_> = tasks
        .into_iter()
        .map(|(table, node)| async move {
            for mut recv in node.recvs {
                let r = recv
                    .recv()
                    .await
                    .ok_or_else(|| format_err!("Channel dropped"))?;
                debug!("recv {} {}", r, table);
            }
            node.task.await?;
            for send in node.sends {
                debug!("send {}", table);
                send.send(table.into()).await?;
            }
            Ok(())
        })
        .collect();
    futures.try_collect().await
}

fn get_parent_graph(
    dag: &HashMap<impl AsRef<str>, BTreeSet<impl AsRef<str>>>,
) -> HashMap<&str, BTreeSet<&str>> {
    dag.iter().fold(HashMap::new(), |mut h, (k, v)| {
        for child in v {
            h.entry(child.as_ref()).or_default().insert(k.as_ref());
        }
        h
    })
}

#[allow(dead_code)]
fn topological_sort(
    dag: &HashMap<StackString, BTreeSet<StackString>>,
) -> Result<Vec<&StackString>, Error> {
    let mut parents_graph = get_parent_graph(dag);
    let mut sorted_elements = Vec::new();
    let mut nodes_without_incoming_edge = Vec::new();
    for key in dag.keys() {
        if !parents_graph.contains_key(key.as_str()) {
            nodes_without_incoming_edge.push(key);
        }
    }
    while let Some(node) = nodes_without_incoming_edge.pop() {
        sorted_elements.push(node);
        if let Some(children) = dag.get(node) {
            for child in children {
                if let Some(parents) = parents_graph.get_mut(child.as_str()) {
                    parents.remove(node.as_str());
                    if parents.is_empty() {
                        nodes_without_incoming_edge.push(child);
                    }
                }
            }
        }
    }
    for node in parents_graph.values() {
        if !node.is_empty() {
            return Err(format_err!("Graph has cycles {:?}", dag));
        }
    }
    Ok(sorted_elements)
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use maplit::{btreeset, hashmap};
    use stack_string::{format_sstr, StackString};
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
    };
    use time::OffsetDateTime;
    use tokio::sync::Mutex;

    use crate::backup_opts::{process_tasks, spawn_threadpool, topological_sort, BackupCommand};

    #[test]
    fn test_backupcommand_display() -> Result<(), Error> {
        let s = format_sstr!(
            "{} {} {}",
            BackupCommand::List,
            BackupCommand::Backup,
            BackupCommand::Restore
        );
        assert_eq!(&s, "list backup restore");
        Ok(())
    }

    #[test]
    fn test_topological_sort() -> Result<(), Error> {
        let ns: Vec<StackString> = (0..8).map(|i| format_sstr!("n{i}").into()).collect();
        let dependencies = hashmap! {
            "n0".into() => btreeset!["n1".into(), "n2".into(), "n3".into()],
            "n1".into() => btreeset!["n4".into(), "n6".into()],
            "n2".into() => btreeset!["n4".into()],
            "n3".into() => btreeset!["n5".into(), "n6".into()],
            "n4".into() => BTreeSet::new(),
            "n5".into() => BTreeSet::new(),
            "n6".into() => btreeset!["n7".into()],
            "n7".into() => btreeset!["n4".into()],
        };
        let expected: Vec<&StackString> = vec![
            &ns[0], &ns[3], &ns[5], &ns[2], &ns[1], &ns[6], &ns[7], &ns[4],
        ];
        let result = topological_sort(&dependencies)?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_process_tasks() -> Result<(), Error> {
        let dependencies: HashMap<StackString, BTreeSet<StackString>> = hashmap! {
            "n0".into() => btreeset!["n1".into(), "n2".into(), "n3".into()],
            "n1".into() => btreeset!["n4".into(), "n6".into()],
            "n2".into() => btreeset!["n4".into()],
            "n3".into() => btreeset!["n5".into(), "n6".into()],
            "n4".into() => BTreeSet::new(),
            "n5".into() => BTreeSet::new(),
            "n6".into() => btreeset!["n7".into()],
            "n7".into() => btreeset!["n4".into()],
        };
        let tasks: Arc<Mutex<Vec<StackString>>> = Arc::new(Mutex::new(Vec::new()));
        process_tasks(&dependencies, |t| {
            let t = t.to_string();
            let tasks = tasks.clone();
            async move {
                tasks.lock().await.push(t.into());
                Ok(())
            }
        })
        .await?;
        println!("{:?}", tasks.lock().await);
        assert_eq!(tasks.lock().await.len(), 8);
        Ok(())
    }

    #[test]
    fn test_topological_sort_cycle() -> Result<(), Error> {
        let deps = hashmap! {
            "n0".into() => btreeset!["n1".into(), "n2".into(), "n3".into()],
            "n1".into() => btreeset!["n4".into(), "n6".into()],
            "n2".into() => btreeset!["n4".into()],
            "n3".into() => btreeset!["n5".into(), "n6".into()],
            "n4".into() => btreeset!["n6".into()],
            "n5".into() => BTreeSet::new(),
            "n6".into() => btreeset!["n1".into()],
        };
        let result = topological_sort(&deps);
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_spawn_threadpool() -> Result<(), Error> {
        let start_time = OffsetDateTime::now_utc();
        let task = spawn_threadpool(move || {
            std::thread::sleep(std::time::Duration::from_millis(1000));
            25
        });
        assert_eq!(task.await?, 25);
        let duration = OffsetDateTime::now_utc() - start_time;
        println!("{}", duration.whole_milliseconds());
        assert!(duration.whole_milliseconds() >= 1000);
        Ok(())
    }
}
