use anyhow::{format_err, Error};
use futures::stream::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use parking_lot::{Mutex, MutexGuard};
use rusoto_core::Region;
use rusoto_s3::{
    Bucket, CopyObjectRequest, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
    GetObjectRequest, Object, PutObjectRequest, S3Client, S3,
};
use s3_ext::S3Ext;
use std::{fmt, path::Path};
use sts_profile_auth::get_client_sts;
use url::Url;
use tokio_compat_02::FutureExt;

lazy_static! {
    static ref S3INSTANCE_TEST_MUTEX: Mutex<()> = Mutex::new(());
}

use stack_string::StackString;

use crate::exponential_retry;

#[derive(Clone)]
pub struct S3Instance {
    s3_client: S3Client,
    max_keys: Option<usize>,
}

impl fmt::Debug for S3Instance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "S3Instance")
    }
}

impl Default for S3Instance {
    fn default() -> Self {
        Self {
            s3_client: get_client_sts!(S3Client, Region::UsEast1).expect("Failed to obtain client"),
            max_keys: None,
        }
    }
}

impl S3Instance {
    pub fn new(aws_region_name: &str) -> Self {
        let region: Region = aws_region_name.parse().ok().unwrap_or(Region::UsEast1);
        Self {
            s3_client: get_client_sts!(S3Client, region).expect("Failed to obtain client"),
            max_keys: None,
        }
    }

    pub fn get_instance_lock() -> MutexGuard<'static, ()> {
        S3INSTANCE_TEST_MUTEX.lock()
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    pub async fn get_list_of_buckets(&self) -> Result<Vec<Bucket>, Error> {
        exponential_retry(|| async move {
            self.s3_client
                .list_buckets().compat()
                .await
                .map(|l| l.buckets.unwrap_or_default())
                .map_err(Into::into)
        })
        .await
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> Result<String, Error> {
        exponential_retry(|| {
            let req = CreateBucketRequest {
                bucket: bucket_name.to_string(),
                ..CreateBucketRequest::default()
            };
            async move {
                self.s3_client
                    .create_bucket(req).compat()
                    .await?
                    .location
                    .ok_or_else(|| format_err!("Failed to create bucket"))
            }
        })
        .await
    }

    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<(), Error> {
        exponential_retry(|| {
            let req = DeleteBucketRequest {
                bucket: bucket_name.to_string(),
            };
            async move { self.s3_client.delete_bucket(req).compat().await.map_err(Into::into) }
        })
        .await
    }

    pub async fn delete_key(&self, bucket_name: &str, key_name: &str) -> Result<(), Error> {
        exponential_retry(|| {
            let req = DeleteObjectRequest {
                bucket: bucket_name.to_string(),
                key: key_name.to_string(),
                ..DeleteObjectRequest::default()
            };
            async move {
                self.s3_client
                    .delete_object(req).compat()
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn copy_key(
        &self,
        source: &Url,
        bucket_to: &str,
        key_to: &str,
    ) -> Result<Option<String>, Error> {
        exponential_retry(|| {
            let copy_source = source.to_string();
            let req = CopyObjectRequest {
                copy_source,
                bucket: bucket_to.to_string(),
                key: key_to.to_string(),
                ..CopyObjectRequest::default()
            };
            async move { self.s3_client.copy_object(req).compat().await.map_err(Into::into) }
        })
        .await
        .map(|x| x.copy_object_result.and_then(|s| s.e_tag))
    }

    pub async fn upload(
        &self,
        fname: impl AsRef<Path>,
        bucket_name: &str,
        key_name: &str,
    ) -> Result<(), Error> {
        let fname = fname.as_ref();
        if !fname.exists() {
            return Err(format_err!("File doesn't exist {:?}", fname));
        }
        exponential_retry(|| {
            let req = PutObjectRequest {
                bucket: bucket_name.to_string(),
                key: key_name.to_string(),
                ..PutObjectRequest::default()
            };
            async move {
                self.s3_client
                    .upload_from_file(fname, req).compat()
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn download(
        &self,
        bucket_name: &str,
        key_name: &str,
        fname: &str,
    ) -> Result<StackString, Error> {
        exponential_retry(|| {
            let req = GetObjectRequest {
                bucket: bucket_name.to_string(),
                key: key_name.to_string(),
                ..GetObjectRequest::default()
            };
            async move {
                self.s3_client
                    .download_to_file(req, fname).compat()
                    .await
                    .map(|x| x.e_tag.as_ref().map_or("", |y| y.trim_matches('"')).into())
                    .map_err(Into::into)
            }
        })
        .await
    }

    pub async fn get_list_of_keys(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>, Error> {
        exponential_retry(|| async move {
            let stream = match prefix {
                Some(p) => self.s3_client.stream_objects_with_prefix(bucket, p),
                None => self.s3_client.stream_objects(bucket),
            };
            let results: Result<Vec<_>, _> = match self.max_keys {
                Some(nkeys) => stream.take(nkeys).try_collect().compat().await,
                None => stream.try_collect().compat().await,
            };
            results.map_err(Into::into)
        })
        .await
    }

    pub async fn process_list_of_keys<T>(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&Object) -> Result<(), Error> + Send + Sync,
    {
        let mut stream = match prefix {
            Some(p) => self.s3_client.stream_objects_with_prefix(bucket, p),
            None => self.s3_client.stream_objects(bucket),
        };
        let mut nkeys = 0;
        while let Some(item) = stream.next().compat().await {
            callback(&item?)?;
            nkeys += 1;
            if let Some(keys) = self.max_keys {
                if nkeys > keys {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
