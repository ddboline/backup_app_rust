#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]

pub mod backup_opts;
pub mod config;
pub mod s3_instance;

use anyhow::Error;
use rand::{
    distr::{Distribution, Uniform},
    rng as thread_rng,
};
use std::{convert::TryFrom, future::Future, time::Duration};
use tokio::time::sleep;

/// # Errors
/// Return error if callback function returns error after timeout
pub async fn exponential_retry<T, U, F>(f: T) -> Result<U, Error>
where
    T: Fn() -> F,
    F: Future<Output = Result<U, Error>>,
{
    let mut timeout: f64 = 1.0;
    let range = Uniform::try_from(0..1000)?;
    loop {
        match f().await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                sleep(Duration::from_millis((timeout * 1000.0) as u64)).await;
                timeout *= 4.0 * f64::from(range.sample(&mut thread_rng())) / 1000.0;
                if timeout >= 64.0 {
                    return Err(err);
                }
            }
        }
    }
}

use time::{OffsetDateTime, macros::format_description};

pub(crate) fn current_date_str() -> String {
    match OffsetDateTime::now_utc()
        .date()
        .format(format_description!("[year][month][day]"))
    {
        Ok(t) => t,
        Err(_) => unreachable!(),
    }
}
