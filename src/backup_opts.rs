use anyhow::{format_err, Error};
use deadqueue::unlimited::Queue;
use derive_more::Display;
use flate2::{read::GzDecoder, Compression, GzBuilder};
use futures::future::try_join_all;
use itertools::Itertools;
use log::{debug, error};
use stack_string::StackString;
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
};
use structopt::StructOpt;
use tempfile::NamedTempFile;
use tokio::sync::oneshot;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc::{
        channel,
        error::TrySendError,
        Receiver, Sender,
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
        send.send(f()).unwrap();
    });
    recv
}

#[derive(StructOpt)]
pub struct BackupOpts {
    /// list, backup, restore
    pub command: BackupCommand,
    #[structopt(short = "f", long)]
    pub config_file: PathBuf,
    #[structopt(short, long)]
    pub key: Option<StackString>,
    #[structopt(short, long)]
    pub num_workers: Option<usize>,
}

impl BackupOpts {
    pub async fn process_args() -> Result<(), Error> {
        let opts = Self::from_args();
        if !opts.config_file.exists() {
            return Err(format_err!(
                "Config file {} does not exist",
                opts.config_file.to_string_lossy()
            ));
        }
        let queue: Arc<Queue<Option<(BackupCommand, StackString, Entry)>>> = Arc::new(Queue::new());
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

        for (key, val) in config {
            if opts.key.is_none() || opts.key.as_ref() == Some(&key) {
                queue.push(Some((opts.command, key, val)));
            }
        }

        worker_tasks.iter().for_each(|_| queue.push(None));

        for task in worker_tasks {
            task.await??;
        }
        Ok(())
    }
}

async fn worker_task(
    queue: &Queue<Option<(BackupCommand, StackString, Entry)>>,
) -> Result<(), Error> {
    while let Some((command, key, entry)) = queue.pop().await {
        process_entry(command, &key, &entry).await?;
    }
    Ok(())
}

async fn process_entry(command: BackupCommand, key: &str, entry: &Entry) -> Result<(), Error> {
    match command {
        BackupCommand::List => {
            println!("{} {}", key, entry);
        }
        BackupCommand::Backup => match entry {
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
                columns,
                ..
            } => {
                let futures = tables.iter().map(|table| async move {
                    let empty = Vec::new();
                    let columns = columns.get(table).unwrap_or(&empty);
                    backup_table(&database_url, &destination, &table, columns).await?;
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
        },
        BackupCommand::Restore => match entry {
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
                columns,
                dependencies,
                sequences,
            } => {
                let mut full_deps = HashMap::new();
                for t in tables {
                    full_deps.insert(t.clone(), BTreeSet::new());
                }
                for t in columns.keys() {
                    full_deps.entry(t.clone()).or_default();
                }
                for (k, v) in dependencies {
                    for child in v {
                        full_deps
                            .entry(k.clone())
                            .or_default()
                            .insert(child.clone());
                        full_deps.entry(child.clone()).or_default();
                    }
                }

                let mut parents_graph = get_parent_graph(&full_deps);
                for t in full_deps.keys() {
                    if !parents_graph.contains_key(t.as_str()) {
                        parents_graph.entry(t.as_str()).or_default();
                    }
                }

                process_tasks(&full_deps, |t| {
                    let t = t.to_string();
                    async move {
                        clear_table(&database_url, &t).await?;
                        Ok(())
                    }
                })
                .await?;

                println!("finished clearing");

                process_tasks(&parents_graph, |t| {
                    let empty = Vec::new();
                    let columns = columns.get(t).unwrap_or(&empty).clone();
                    let t = t.to_string();
                    async move {
                        restore_table(&database_url, &destination, &t, &columns).await?;
                        Ok(())
                    }
                })
                .await?;

                restore_sequences(&database_url, sequences).await?;
                println!("Finished postgres_restore {}", key);
            }
            Entry::Local {
                require_sudo,
                destination,
                ..
            } => {
                run_local_restore(*require_sudo, destination).await?;
                println!("Finished local_restore {}", key);
            }
        },
    };
    Ok(())
}

async fn run_local_backup(
    require_sudo: bool,
    destination: &Url,
    backup_paths: &[impl AsRef<Path>],
    command_output: &[(impl AsRef<str>, impl AsRef<str>)],
    exclude: &[impl AsRef<str>],
) -> Result<(), Error> {
    let user = std::env::var("USER").unwrap_or_else(|_| "root".to_string());
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
    let backup_paths: Vec<_> = backup_paths
        .iter()
        .map(|p| p.as_ref().to_string_lossy().into_owned())
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
        args.push(output_filename.as_ref().to_string());
    }
    if !exclude.is_empty() {
        for ex in exclude {
            args.push(format!("--exclude={}", ex.as_ref()));
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

    p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

    if require_sudo {
        let output = Command::new("sudo")
            .args(&["chown", &format!("{u}:{u}", u = user), destination])
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

    p.wait().await?;
    stdout_task.await??;
    stderr_task.await??;

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
        Path::new(destination.path()).join(format!("{}.sql.gz", table))
    } else {
        tempfile.path().to_path_buf()
    };
    let query = if columns.is_empty() {
        format!("COPY {} TO STDOUT", table)
    } else {
        format!(
            "COPY {} ({}) TO STDOUT",
            table,
            columns.iter().map(AsRef::as_ref).join(",")
        )
    };

    let mut p = Command::new("psql")
        .args(&[database_url.as_str(), "-c", &query])
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
    p.wait().await?;
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
    let query = if columns.is_empty() {
        format!("COPY {} FROM STDIN", table)
    } else {
        format!(
            "COPY {} ({}) FROM STDIN",
            table,
            columns.iter().map(AsRef::as_ref).join(",")
        )
    };

    let mut p = Command::new("psql")
        .args(&[database_url.as_str(), "-c", &query])
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

    p.wait().await?;
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
    p.wait().await?;
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
    p.wait().await?;
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

    let input_file = File::open(&input_path)?;
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
    let output_file = File::create(&output_path)?;
    let mut gz = GzBuilder::new()
        .filename(file_name)
        .write(output_file, Compression::default());
    loop {
        match recv.blocking_recv() {
            Some(buf) => {
                gz.write_all(&buf)?;
            },
            None => break,
        }
    }
    gz.try_finish()?;
    Ok(())
}

#[derive(Display, Clone, Copy)]
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
            tasks.get_mut(key.as_ref()).unwrap().sends.push(s);
            tasks.get_mut(child.as_ref()).unwrap().recvs.push(r);
        }
    }

    let futures = tasks.into_iter().map(|(table, node)| async move {
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
    });
    let results: Result<Vec<_>, Error> = try_join_all(futures).await;
    results?;

    Ok(())
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
    use stack_string::StackString;
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
    };
    use tokio::sync::Mutex;

    use crate::backup_opts::{process_tasks, topological_sort, BackupCommand};

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

    #[test]
    fn test_topological_sort() -> Result<(), Error> {
        let ns: Vec<StackString> = (0..8).map(|i| format!("n{}", i).into()).collect();
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
}
