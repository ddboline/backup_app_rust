use anyhow::{format_err, Error};
use chrono::Utc;
use derive_more::{Deref, DerefMut, Into, IntoIterator};
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};
use url::Url;

lazy_static! {
    pub static ref HOME: PathBuf = dirs::home_dir().expect("No Home Directory");
}

#[derive(Debug, PartialEq, IntoIterator, Deref, DerefMut)]
pub struct Config(Vec<(StackString, Entry)>);

impl Config {
    pub fn new(p: &Path) -> Result<Self, Error> {
        let data = fs::read(p)?;
        let config: ConfigToml = toml::from_slice(&data)?;
        config.try_into()
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (k, v) in &self.0 {
            writeln!(f, "{}: {}", k, v)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum Entry {
    Postgres {
        database_url: Url,
        destination: Url,
        tables: Vec<StackString>,
        columns: HashMap<StackString, Vec<StackString>>,
        dependencies: HashMap<StackString, Vec<StackString>>,
        sequences: HashMap<StackString, (StackString, StackString)>,
    },
    Local {
        require_sudo: bool,
        destination: Url,
        backup_paths: Vec<PathBuf>,
        command_output: Vec<(StackString, StackString)>,
        exclude: Vec<StackString>,
    },
    FullPostgresBackup {
        destination: Url,
    },
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Entry ")?;
        match self {
            Self::Postgres {
                database_url,
                destination,
                tables,
                columns,
                dependencies,
                sequences,
            } => {
                write!(
                    f,
                    "postgres\n\tdb_url: {database_url}\n\tdest: {destination}\n{t}{c}{d}{s}",
                    t = if tables.is_empty() {
                        "".into()
                    } else {
                        format_sstr!("\ttables: {}\n", tables.join(", "))
                    },
                    c = if columns.is_empty() {
                        "".into()
                    } else {
                        format_sstr!(
                            "\tcolumns: {}\n",
                            columns
                                .iter()
                                .map(|(k, v)| { format_sstr!("{}: [{}]", k, v.join(",")) })
                                .join(", ")
                        )
                    },
                    d = if dependencies.is_empty() {
                        "".into()
                    } else {
                        format_sstr!(
                            "\tdependencies: {}\n",
                            dependencies
                                .iter()
                                .map(|(k, v)| { format_sstr!("{}: [{}]", k, v.join(",")) })
                                .join(", ")
                        )
                    },
                    s = if sequences.is_empty() {
                        "".into()
                    } else {
                        format_sstr!(
                            "\tsequences: {}\n",
                            sequences
                                .iter()
                                .map(|(k, (a, b))| format_sstr!("{k} {a} {b}"))
                                .join(", ")
                        )
                    }
                )
            }
            Self::Local {
                require_sudo,
                destination,
                backup_paths,
                command_output,
                exclude,
            } => {
                write!(
                    f,
                    "local\n\tsudo: {require_sudo}\n\tdest: {destination}\n\tpaths: {bp}\n{cm}{ex}",
                    bp = backup_paths.iter().map(|p| p.to_string_lossy()).join(", "),
                    cm = if command_output.is_empty() {
                        "".into()
                    } else {
                        format_sstr!(
                            "\tcommand: {}\n",
                            command_output
                                .iter()
                                .map(|(a, b)| format_sstr!("{a} {b}"))
                                .join(", ")
                        )
                    },
                    ex = if exclude.is_empty() {
                        "".into()
                    } else {
                        format_sstr!("\texclude: {}\n", exclude.join(", "))
                    }
                )
            }
            Self::FullPostgresBackup { destination } => {
                write!(f, "full_postgres_backup {destination}")
            }
        }
    }
}

impl TryFrom<ConfigToml> for Config {
    type Error = Error;
    fn try_from(item: ConfigToml) -> Result<Self, Self::Error> {
        let result: Result<_, Error> = item
            .into_iter()
            .map(|(key, entry)| {
                let entry = entry.try_into()?;
                Ok((key.into(), entry))
            })
            .collect();
        Ok(Self(result?))
    }
}

impl TryFrom<EntryToml> for Entry {
    type Error = Error;
    fn try_from(entry: EntryToml) -> Result<Self, Self::Error> {
        let destination = entry
            .destination
            .ok_or_else(|| format_err!("No destination"))?
            .into();
        let require_sudo = entry.require_sudo.unwrap_or(false);
        let full_postgres_backup = entry.full_postgres_backup.unwrap_or(false);
        let sequences = entry
            .sequences
            .unwrap_or_else(HashMap::new)
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();
        let command_output = entry.command_output.unwrap_or_else(Vec::new);
        let exclude = entry.exclude.unwrap_or_else(Vec::new);
        if full_postgres_backup {
            return Ok(Self::FullPostgresBackup { destination });
        } else if let Some(database_url) = entry.database_url {
            if let Some(tables) = entry.tables {
                let dependencies = entry
                    .dependencies
                    .unwrap_or_else(HashMap::new)
                    .into_iter()
                    .map(|(k, mut v)| {
                        v.sort();
                        v.dedup();
                        (k.into(), v)
                    })
                    .collect();
                if let Some(columns) = entry.columns {
                    let columns: HashMap<_, _> =
                        columns.into_iter().map(|(k, v)| (k.into(), v)).collect();
                    let tables = tables
                        .iter()
                        .chain(columns.keys())
                        .sorted()
                        .dedup()
                        .cloned()
                        .collect();
                    return Ok(Self::Postgres {
                        database_url: database_url.into(),
                        destination,
                        tables,
                        columns,
                        dependencies,
                        sequences,
                    });
                }
                return Ok(Self::Postgres {
                    database_url: database_url.into(),
                    destination,
                    tables,
                    columns: HashMap::new(),
                    dependencies,
                    sequences,
                });
            }
        } else if let Some(backup_paths) = entry.backup_paths {
            let backup_paths = backup_paths
                .into_iter()
                .map(|p| {
                    if p.exists() {
                        Ok(p)
                    } else {
                        let new_p = HOME.join(&p);
                        if new_p.exists() {
                            Ok(new_p)
                        } else {
                            Err(format_err!("Path {:?} does not exist", p))
                        }
                    }
                })
                .collect::<Result<Vec<_>, Error>>()?;
            return Ok(Self::Local {
                require_sudo,
                destination,
                backup_paths,
                command_output,
                exclude,
            });
        }
        Err(format_err!("Bad config format"))
    }
}

type ConfigToml = HashMap<String, EntryToml>;

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct EntryToml {
    full_postgres_backup: Option<bool>,
    require_sudo: Option<bool>,
    database_url: Option<UrlWrapper>,
    destination: Option<UrlWrapper>,
    backup_paths: Option<Vec<PathBuf>>,
    tables: Option<Vec<StackString>>,
    columns: Option<HashMap<String, Vec<StackString>>>,
    dependencies: Option<HashMap<String, Vec<StackString>>>,
    sequences: Option<HashMap<String, (StackString, StackString)>>,
    command_output: Option<Vec<(StackString, StackString)>>,
    exclude: Option<Vec<StackString>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Into, PartialEq)]
#[serde(into = "String", try_from = "&str")]
pub struct UrlWrapper(Url);

impl UrlWrapper {
    fn replace_date(s: &str) -> Cow<str> {
        if s.contains("DATE") {
            let date = StackString::from_display(Utc::now().format("%Y%m%d"));
            s.replace("DATE", &date).into()
        } else {
            s.into()
        }
    }

    fn replace_sysid(s: &str) -> Result<Cow<str>, Error> {
        if s.contains("SYSID") {
            let date = Utc::now().format("%Y%m%d");
            let sysid: Vec<u8> = std::process::Command::new("uname")
                .args(&["-snrmpio"])
                .output()?
                .stdout
                .into_iter()
                .map(|c| match c {
                    b' ' | b'/' | b'.' => b'_',
                    x => x,
                })
                .collect();
            let sysid = String::from_utf8(sysid)?;
            let sysid = format_sstr!("{sysid}_{date}");
            Ok(s.replace("SYSID", &sysid).into())
        } else {
            Ok(s.into())
        }
    }
}

impl From<UrlWrapper> for String {
    fn from(item: UrlWrapper) -> String {
        item.0.into()
    }
}

impl TryFrom<&str> for UrlWrapper {
    type Error = Error;
    fn try_from(item: &str) -> Result<Self, Self::Error> {
        let s = UrlWrapper::replace_date(item);
        let url: Url = UrlWrapper::replace_sysid(&s)?.parse()?;
        Ok(Self(url))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use chrono::Utc;
    use log::debug;
    use maplit::hashmap;
    use stack_string::{format_sstr, StackString};
    use std::{
        convert::TryInto,
        fmt::Write,
        fs::{create_dir_all, remove_dir_all},
    };

    use crate::config::{Config, ConfigToml, EntryToml, UrlWrapper};

    #[test]
    fn test_config() -> Result<(), Error> {
        let home_dir = dirs::home_dir().expect("No HOME directory");

        let database_url: UrlWrapper =
            "postgresql://user:password@localhost:5432/aws_app_cache".try_into()?;
        let tables = vec!["instance_family".into(), "instance_list".into()];
        let columns = hashmap! {
            "instance_family".into() => vec!["id".into(), "family_name".into()],
        };
        let destination = "s3://aws-app-rust-db-backup".try_into()?;
        let sequences = hashmap! {
            "intrusion_log_id_seq".into() => ("intrusion_log".into(), "id".into()),
        };
        let aws_entry = EntryToml {
            database_url: Some(database_url),
            tables: Some(tables),
            columns: Some(columns),
            destination: Some(destination),
            sequences: Some(sequences),
            ..EntryToml::default()
        };

        let database_url: UrlWrapper =
            "postgresql://user:password@localhost:5432/calendar_app_cache".try_into()?;
        let tables = vec!["calendar_cache".into(), "calendar_list".into()];
        let destination = "s3://calendar-app-rust-db-backup".try_into()?;
        let calendar_entry = EntryToml {
            database_url: Some(database_url),
            tables: Some(tables),
            destination: Some(destination),
            ..EntryToml::default()
        };

        let database_url: UrlWrapper =
            "postgresql://user:password@localhost:5432/movie_queue".try_into()?;
        let tables = vec![
            "imdb_ratings".into(),
            "imdb_episodes".into(),
            "movie_collection_on_dvd".into(),
            "movie_collection".into(),
            "movie_queue".into(),
            "trakt_watched_episodes".into(),
            "trakt_watched_movies".into(),
            "trakt_watchlist".into(),
        ];
        let destination = "s3://movie-queue-db-backup".try_into()?;
        let sequences = hashmap! {
            "imdb_ratings_id_seq".into() => ("imdb_ratings".into(), "index".into()),
            "imdb_episodes_id_seq".into() => ("imdb_episodes".into(), "id".into()),
            "trakt_watched_episodes_id_seq".into() => ("trakt_watched_episodes".into(), "id".into()),
            "trakt_watched_movies_id_seq".into() => ("trakt_watched_movies".into(), "id".into()),
            "trakt_watchlist_id_seq".into() => ("trakt_watchlist".into(), "id".into()),
        };
        let dependencies = hashmap! {
            "imdb_episodes".into() => vec!["imdb_ratings".into()],
            "movie_collection".into() => vec!["imdb_ratings".into()],
            "movie_queue".into() => vec!["movie_collection".into()],
        };
        let movie_entry = EntryToml {
            database_url: Some(database_url),
            tables: Some(tables),
            destination: Some(destination),
            sequences: Some(sequences),
            dependencies: Some(dependencies),
            ..EntryToml::default()
        };

        let date = StackString::from_display(Utc::now().format("%Y%m%d"));
        let sysid: Vec<u8> = std::process::Command::new("uname")
            .args(&["-snrmpio"])
            .output()?
            .stdout
            .into_iter()
            .map(|c| match c {
                b' ' | b'/' | b'.' => b'_',
                x => x,
            })
            .collect();
        let sysid = String::from_utf8(sysid)?;
        let sysid = format_sstr!("{sysid}_{date}");

        create_dir_all(home_dir.join("test_backup_app"))?;

        let backup_paths = vec![home_dir.join("test_backup_app")];
        let destination = format_sstr!(
            "file://{h}/temp_{sysid}_{date}.tar.gz",
            h = home_dir.to_string_lossy()
        )
        .as_str()
        .try_into()?;
        let local_entry = EntryToml {
            destination: Some(destination),
            backup_paths: Some(backup_paths),
            require_sudo: Some(true),
            ..EntryToml::default()
        };
        let entries = hashmap! {
            "Dropbox".into() => local_entry,
            "aws_app_rust".into() => aws_entry,
            "calendar_app_rust".into() => calendar_entry,
            "movie_collection_rust".into() => movie_entry,
        };
        debug!("{}", toml::to_string_pretty(&entries)?);
        let mut config: Config = entries.try_into()?;
        config.sort_by(|x, y| x.0.cmp(&y.0));
        debug!("{:?}", config);

        let home_dir = dirs::home_dir().expect("No HOME directory");

        let data = include_str!("../tests/data/test_config.toml")
            .replace("HOME", &home_dir.to_string_lossy());
        let config_file: ConfigToml = toml::from_str(&data)?;
        let mut config_file: Config = config_file.try_into()?;
        config_file.sort_by(|x, y| x.0.cmp(&y.0));
        debug!("{}", config_file.len());
        debug!("{}", config.len());
        assert_eq!(config_file, config);

        remove_dir_all(home_dir.join("test_backup_app"))?;

        Ok(())
    }
}
