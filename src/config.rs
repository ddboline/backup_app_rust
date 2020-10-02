use stack_string::StackString;
use std::path::PathBuf;
use url::Url;
use serde::{Serialize, Deserialize};
use anyhow::Error;
use std::convert::TryFrom;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    postgres_entries: HashMap<StackString, PostgresEntry>,
    local_entries: HashMap<StackString, LocalEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PostgresEntry {
    database_url: UrlWrapper,
    table_name: StackString,
    destination: UrlWrapper,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LocalEntry {
        require_sudo: bool,
        destination: UrlWrapper,
        backup_paths: Vec<PathBuf>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(into="String", try_from="&str")]
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
    use toml::to_string_pretty;
    use std::convert::TryInto;
    use maplit::hashmap;

    use crate::config::{Config, PostgresEntry, UrlWrapper, LocalEntry};

    #[test]
    fn test_config() -> Result<(), Error> {
        let database_url: UrlWrapper = "postgresql://ddboline:Gdf3m895Nip5pydr@localhost:5432/aws_app_cache".try_into()?;
        let table_name = "instance_family".into();
        let destination = "s3://aws-app-rust-db-backup".try_into()?;
        let entry = PostgresEntry {database_url, table_name, destination};
        let postgres_entries = hashmap!{"aws_app_rust".into() => entry};
        
        let backup_paths = vec!["/home/ddboline/Dropbox".into()];
        let destination = "file:///home/ddboline/temp.tar.gz".try_into()?;
        let entry = LocalEntry {require_sudo: false, destination, backup_paths};
        let local_entries = hashmap!{"Dropbox".into() => entry};
        let config = Config {postgres_entries, local_entries};
        println!("{:?}", config);
        println!("{}", to_string_pretty(&config)?);
        assert!(false);
        Ok(())
    }
}
