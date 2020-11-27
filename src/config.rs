use anyhow::{format_err, Error};
use chrono::Utc;
use derive_more::Into;
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use stack_string::StackString;
use std::borrow::Cow;
use std::{
    collections::{hash_map::IntoIter, HashMap, HashSet},
    convert::{TryFrom, TryInto},
    fmt, fs,
    path::{Path, PathBuf},
};
use url::Url;

lazy_static! {
    pub static ref HOME: PathBuf = dirs::home_dir().expect("No Home Directory");
}

#[derive(Debug, PartialEq)]
pub struct Config(HashMap<StackString, Entry>);

impl Config {
    pub fn new(p: &Path) -> Result<Self, Error> {
        let data = fs::read_to_string(p)?;
        let config: ConfigToml = toml::from_str(&data)?;
        config.try_into()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StackString, &Entry)> {
        self.0.iter()
    }
}

impl IntoIterator for Config {
    type Item = (StackString, Entry);
    type IntoIter = IntoIter<StackString, Entry>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
        let s = match self {
            Self::Postgres {
                database_url,
                destination,
                tables,
                columns,
                dependencies,
                sequences,
            } => format!(
                "postgres\n\tdb_url: {}\n\tdest: {}\n{}{}{}{}",
                database_url.as_str(),
                destination.as_str(),
                if tables.is_empty() {
                    "".to_string()
                } else {
                    format!("\ttables: {}\n", tables.join(", "))
                },
                if columns.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "\tcolumns: {}\n",
                        columns
                            .iter()
                            .map(|(k, v)| { format!("{}: [{}]", k, v.join(",")) })
                            .join(", ")
                    )
                },
                if dependencies.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "\tdependencies: {}\n",
                        dependencies
                            .iter()
                            .map(|(k, v)| { format!("{}: [{}]", k, v.join(",")) })
                            .join(", ")
                    )
                },
                if sequences.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "\tsequences: {}\n",
                        sequences
                            .iter()
                            .map(|(k, (a, b))| format!("{} {} {}", k, a, b))
                            .join(", ")
                    )
                }
            ),
            Self::Local {
                require_sudo,
                destination,
                backup_paths,
                command_output,
                exclude,
            } => format!(
                "local\n\tsudo: {}\n\tdest: {}\n\tpaths: {}\n{}{}",
                require_sudo,
                destination.as_str(),
                backup_paths.iter().map(|p| p.to_string_lossy()).join(", "),
                if command_output.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "\tcommand: {}\n",
                        command_output
                            .iter()
                            .map(|(a, b)| format!("{} {}", a, b))
                            .join(", ")
                    )
                },
                if exclude.is_empty() {
                    "".to_string()
                } else {
                    format!("\texclude: {}\n", exclude.join(", "))
                }
            ),
            Self::FullPostgresBackup { destination } => {
                format!("full_postgres_backup {}", destination.as_str())
            }
        };
        write!(f, "Entry {}", s)
    }
}

impl TryFrom<ConfigToml> for Config {
    type Error = Error;
    fn try_from(item: ConfigToml) -> Result<Self, Self::Error> {
        let result: Result<_, Error> = item
            .into_iter()
            .map(|(key, entry)| {
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
                    return Ok((key.into(), Entry::FullPostgresBackup { destination }));
                } else if let Some(database_url) = entry.database_url {
                    if let Some(tables) = entry.tables {
                        let dependencies = entry
                            .dependencies
                            .unwrap_or(HashMap::new())
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
                                .map(|x| x.clone())
                                .collect();
                            return Ok((
                                key.into(),
                                Entry::Postgres {
                                    database_url: database_url.into(),
                                    destination,
                                    tables,
                                    columns,
                                    dependencies,
                                    sequences,
                                },
                            ));
                        } else {
                            return Ok((
                                key.into(),
                                Entry::Postgres {
                                    database_url: database_url.into(),
                                    destination,
                                    tables,
                                    columns: HashMap::new(),
                                    dependencies,
                                    sequences,
                                },
                            ));
                        }
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
                    return Ok((
                        key.into(),
                        Entry::Local {
                            require_sudo,
                            destination,
                            backup_paths,
                            command_output,
                            exclude,
                        },
                    ));
                }
                Err(format_err!("Bad config format"))
            })
            .collect();
        Ok(Self(result?))
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
            let date = Utc::now().format("%Y%m%d").to_string();
            s.replace("DATE", &date).into()
        } else {
            s.into()
        }
    }

    fn replace_sysid(s: &str) -> Result<Cow<str>, Error> {
        if s.contains("SYSID") {
            let date = Utc::now().format("%Y%m%d").to_string();
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
            let sysid = format!("{}_{}", sysid, date);
            Ok(s.replace("SYSID", &sysid).into())
        } else {
            Ok(s.into())
        }
    }
}

impl From<UrlWrapper> for String {
    fn from(item: UrlWrapper) -> String {
        item.0.into_string()
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
    use maplit::hashmap;
    use std::{convert::TryInto, fs};

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

        let date = Utc::now().format("%Y%m%d").to_string();
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
        let sysid = format!("{}_{}", sysid, date);

        let backup_paths = vec![home_dir.join("Dropbox")];
        let destination = format!(
            "file://{}/temp_{}_{}.tar.gz",
            home_dir.to_string_lossy(),
            sysid,
            date
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
        };
        println!("{}", toml::to_string_pretty(&entries)?);
        let config: Config = entries.try_into()?;
        println!("{:?}", config);

        let home_dir = dirs::home_dir().expect("No HOME directory");

        let data = include_str!("../tests/data/test_config.toml")
            .replace("HOME", &home_dir.to_string_lossy());
        let config_file: ConfigToml = toml::from_str(&data)?;
        let config_file: Config = config_file.try_into()?;
        println!("{}", config_file);
        assert_eq!(config_file, config);
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_local() -> Result<(), Error> {
        let home_dir = dirs::home_dir().expect("No HOME directory");
        let p = home_dir
            .join(".config")
            .join("backup_app_rust")
            .join("postgres.toml");
        let data = fs::read_to_string(&p)?;
        let config_postgres: ConfigToml = toml::from_str(&data)?;
        let config_postgres: Config = config_postgres.try_into()?;
        println!("{:?}", config_postgres);

        let p = home_dir
            .join(".config")
            .join("backup_app_rust")
            .join("local_home_backup.toml");
        let data = fs::read_to_string(&p)?;
        let config_local_home: ConfigToml = toml::from_str(&data)?;
        let config_local_home: Config = config_local_home.try_into()?;
        println!("{:?}", config_local_home);

        let p = home_dir
            .join(".config")
            .join("backup_app_rust")
            .join("local_backup.toml");
        let data = fs::read_to_string(&p)?;
        let config_local: ConfigToml = toml::from_str(&data)?;
        let config_local: Config = config_local.try_into()?;
        println!("{:?}", config_local);

        Ok(())
    }
}
