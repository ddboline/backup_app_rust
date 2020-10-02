use anyhow::Error;
use serde::{Deserialize, Serialize};
use stack_string::StackString;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;
use url::Url;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    stuff: Option<StackString>,
    entries: HashMap<StackString, Entry>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entry {
    require_sudo: Option<bool>,
    database_url: Option<UrlWrapper>,
    destination: Option<UrlWrapper>,
    backup_paths: Option<Vec<PathBuf>>,
    tables: Option<Vec<StackString>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(into = "String", try_from = "&str")]
pub struct UrlWrapper(Url);

impl From<UrlWrapper> for String {
    fn from(item: UrlWrapper) -> String {
        item.0.into_string()
    }
}

impl TryFrom<&str> for UrlWrapper {
    type Error = Error;
    fn try_from(item: &str) -> Result<Self, Self::Error> {
        let url: Url = item.parse()?;
        Ok(Self(url))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use maplit::hashmap;
    use std::convert::TryInto;
    use toml::to_string_pretty;

    use crate::config::{Config, Entry, UrlWrapper};

    #[test]
    fn test_config() -> Result<(), Error> {
        let database_url: UrlWrapper =
            "postgresql://user:password@localhost:5432/aws_app_cache".try_into()?;
        let tables = vec!["instance_family".into(), "instance_list".into()];
        let destination = "s3://aws-app-rust-db-backup".try_into()?;
        let aws_entry = Entry {
            database_url: Some(database_url),
            tables: Some(tables),
            destination: Some(destination),
            ..Entry::default()
        };

        let database_url: UrlWrapper =
            "postgresql://user:password@localhost:5432/calendar_app_cache"
                .try_into()?;
        let tables = vec!["calendar_cache".into(), "calendar_list".into()];
        let destination = "s3://calendar-app-rust-db-backup".try_into()?;
        let calendar_entry = Entry {
            database_url: Some(database_url),
            tables: Some(tables),
            destination: Some(destination),
            ..Entry::default()
        };

        let backup_paths = vec!["/home/ddboline/Dropbox".into()];
        let destination = "file:///home/ddboline/temp.tar.gz".try_into()?;
        let local_entry = Entry {
            destination: Some(destination),
            backup_paths: Some(backup_paths),
            ..Entry::default()
        };
        let entries = hashmap! {
            "Dropbox".into() => local_entry,
            "aws_app_rust".into() => aws_entry,
            "calendar_app_rust".into() => calendar_entry,
        };
        let config = Config {
            entries,
            ..Config::default()
        };
        println!("{:?}", config);
        println!("{}", to_string_pretty(&config)?);
        assert!(false);
        Ok(())
    }
}
