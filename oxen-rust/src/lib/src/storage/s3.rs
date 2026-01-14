use crate::error::OxenError;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::{config::Region, error::SdkError, Client};
use bytes::Bytes;
use futures::StreamExt;
use http_body::Frame;
use image::DynamicImage;
use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_util::io::ReaderStream;

use super::version_store::{VersionStore, VersionWriter};
use crate::constants::VERSION_FILE_NAME;
use crate::view::versions::{CleanCorruptedVersionsResult, CompleteFileChunk};

/// S3 implementation of version storage
#[derive(Debug)]
pub struct S3VersionStore {
    client: OnceCell<Result<Arc<Client>, OxenError>>,
    bucket: String,
    prefix: String,
}

impl S3VersionStore {
    /// Create a new S3VersionStore
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `prefix` - Prefix for all objects in the bucket
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            client: OnceCell::new(),
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    pub async fn init_client(&self) -> Result<Arc<Client>, OxenError> {
        let result_ref = self
            .client
            .get_or_init(|| async {
                // Create a temp client to get the bucket region
                let region_provider = RegionProviderChain::default_provider().or_else("us-west-1");

                let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(region_provider)
                    .load()
                    .await;
                let tmp_client = Client::new(&base_config);

                let detected_region = tmp_client
                    .get_bucket_location()
                    .bucket(&self.bucket)
                    .send()
                    .await
                    .map_err(|err| {
                        OxenError::basic_str(format!(
                            "S3 init_client Failed to get bucket location: {:?}",
                            parse_s3_error(&err)
                        ))
                    })?
                    .location_constraint()
                    .map(|loc| loc.as_str().to_string())
                    .unwrap_or("us-east-1".to_string());

                // Construct the client with the detected bucket region
                let real_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(detected_region))
                    .load()
                    .await;

                Ok::<Arc<Client>, OxenError>(Arc::new(Client::new(&real_config)))
            })
            .await;

        match result_ref {
            Ok(client) => Ok(client.clone()),
            Err(e) => Err(OxenError::basic_str(format!("{e:?}"))),
        }
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> String {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        // S3 dir key ends with / to avoid mismatching other files with the same prefix
        format!("{}/{}/{}/", self.prefix, topdir, subdir)
    }

    /// Get the full path for a version file
    fn generate_key(&self, hash: &str) -> String {
        format!("{}{}", self.version_dir(hash), VERSION_FILE_NAME)
    }
}

#[async_trait]
impl VersionStore for S3VersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        let client = self.init_client().await?;

        // Check permission to write to S3
        match client.head_bucket().bucket(&self.bucket).send().await {
            Ok(result) => {
                log::debug!("Successfully got S3 bucket {result:?}");
                let test_key = format!("{}/_permission_check", self.prefix);
                let body = ByteStream::from("permission-check".as_bytes().to_vec());

                match client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&test_key)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(_) => {
                        client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(&test_key)
                            .send()
                            .await
                            .map_err(|err| {
                                OxenError::basic_str(format!(
                                    "S3 init() failed to delete _permission_check: {}",
                                    parse_s3_error(&err)
                                ))
                            })?;
                        Ok(())
                    }
                    // Surface the error from S3
                    Err(err) => Err(OxenError::basic_str(format!(
                        "S3 init() write permission check failed: {}",
                        parse_s3_error(&err)
                    ))),
                }
            }
            Err(err) => Err(OxenError::basic_str(format!(
                "S3 init() cannot access S3 bucket '{}': {}",
                self.bucket,
                parse_s3_error(&err)
            ))),
        }
    }

    async fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError> {
        // get the client
        let client = self.init_client().await?;
        // get file content from the path
        let mut file = std::fs::File::open(file_path).map_err(|e| {
            OxenError::basic_str(format!("Failed to open file {}: {e}", file_path.display()))
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            OxenError::basic_str(format!("Failed to read file {}: {e}", file_path.display()))
        })?;

        let key = self.generate_key(hash);
        let body = ByteStream::from(buffer);
        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 store_version_from_path failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        Ok(())
    }

    // It's only used in the client side for version file download
    async fn store_version_from_reader(
        &self,
        _hash: &str,
        _reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError> {
        Err(OxenError::basic_str(
            "S3VersionStore store_version_from_reader not yet implemented",
        ))
    }

    async fn store_version_from_reader_with_size(
        &self,
        hash: &str,
        reader: Box<dyn AsyncRead + Send + Sync + Unpin>,
        size: u64,
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        log::debug!("Storing version to S3 from reader with size {size}");
        let key = self.generate_key(hash);

        let stream =
            ReaderStream::with_capacity(reader, 64 * 1024) // 64KB buffer
                .map(|result| result.map(Frame::data).map_err(std::io::Error::other));

        let sdk_body = SdkBody::from_body_1_x(http_body_util::StreamBody::new(stream));
        let byte_stream = ByteStream::from_body_1_x(sdk_body);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(byte_stream)
            .content_length(size as i64)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 store_version_from_reader_with_size failed : {}",
                    parse_s3_error(&e)
                ))
            })?;

        Ok(())
    }

    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        log::debug!("Storing version to S3");
        let key = self.generate_key(hash);

        let body = ByteStream::from(data.to_vec());
        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!("S3 store_version failed: {}", parse_s3_error(&e)))
            })?;

        Ok(())
    }

    async fn store_version_derived(
        &self,
        _derived_image: DynamicImage,
        image_buf: Vec<u8>,
        derived_path: &Path,
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = derived_path
            .components()
            .filter_map(|c| match c {
                Component::Normal(c) => Some(c.to_string_lossy()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/");

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(image_buf))
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 store_version_derived failed: {}",
                    parse_s3_error(&e)
                ))
            })?;
        log::debug!("Saved derived version file {derived_path:?}");

        Ok(())
    }

    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 get_version_size failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        let size = resp
            .content_length()
            .ok_or_else(|| OxenError::basic_str("S3 object missing content_length"))?
            as u64;
        Ok(size)
    }

    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!("S3 get_object failed: {}", parse_s3_error(&e)))
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 read response body failed: {e}")))?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn get_version_stream(
        &self,
        hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 get_version_stream failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        let adapter = ByteStreamAdapter { inner: resp.body };

        Ok(Box::new(adapter) as Box<_>)
    }

    async fn get_version_derived_stream(
        &self,
        derived_path: &Path,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = derived_path
            .components()
            .filter_map(|c| match c {
                Component::Normal(c) => Some(c.to_string_lossy()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/");

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 get_version_derived_stream failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        let adapter = ByteStreamAdapter { inner: resp.body };
        Ok(Box::new(adapter) as Box<_>)
    }

    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError> {
        let key = self.generate_key(hash);
        Ok(PathBuf::from(key))
    }

    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        let mut stream = self.get_version_stream(hash).await?;
        let mut file = File::create(dest_path).await?;

        while let Some(chunk) = stream.next().await {
            let data = chunk.map_err(|e| {
                OxenError::basic_str(format!("Failed to read chunk from S3 version stream: {e}"))
            })?;
            file.write_all(&data).await.map_err(|e| {
                OxenError::basic_str(format!(
                    "Failed to write chunk to file {}: {e}",
                    dest_path.display()
                ))
            })?;
        }

        file.flush().await?;

        Ok(())
    }

    async fn init_version_chunk_session(&self, hash: &str) -> Result<Option<String>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let upload_resp = client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 create_multipart_upload failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        let upload_id = upload_resp
            .upload_id()
            .ok_or_else(|| OxenError::basic_str("Missing upload_id"))?;

        Ok(Some(upload_id.to_string()))
    }

    // It's only used in the entries client for pull
    async fn store_version_chunk(
        &self,
        _hash: &str,
        _upload_id: Option<&str>,
        _offset: u64,
        _chunk_number: Option<i32>,
        _data: &[u8],
    ) -> Result<(), OxenError> {
        Err(OxenError::basic_str(
            "S3VersionStore store_version_chunk not yet implemented",
        ))
    }

    // Must call finish() specifically before the writer dropped
    async fn get_version_chunk_writer(
        &self,
        hash: &str,
        upload_id: Option<&str>,
        _offset: u64,
        chunk_number: Option<i32>,
        chunk_size: Option<u64>,
    ) -> Result<Box<dyn VersionWriter>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);
        let upload_id = upload_id
            .ok_or_else(|| OxenError::basic_str("Missing upload_id"))?
            .to_string();
        let chunk_size = chunk_size.ok_or_else(|| OxenError::basic_str("Missing chunk_size"))?;
        let chunk_number = chunk_number.ok_or_else(|| {
            OxenError::basic_str("Missing chunk_number for S3 store_version_chunk_stream")
        })?;
        let bucket = self.bucket.clone();
        let (tx, rx) = duplex(64 * 1024);

        let stream = ReaderStream::new(rx)
            .map(|result| result.map(Frame::data).map_err(std::io::Error::other));

        let sdk_body = SdkBody::from_body_1_x(http_body_util::StreamBody::new(stream));

        let byte_stream = ByteStream::from_body_1_x(sdk_body);

        let upload_handle = tokio::spawn(async move {
            client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(chunk_number + 1)
                .content_length(chunk_size as i64)
                .body(byte_stream)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!("S3 chunk upload failed: {}", parse_s3_error(&e)))
                })?;
            Ok(())
        });

        Ok(Box::new(S3VersionWriter {
            writer: tx,
            upload_handle,
        }))
    }

    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let end = offset + size - 1;
        let range = format!("bytes={offset}-{end}");

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 get_version_chunk failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        let mut reader = resp.body.into_async_read();
        let mut buf = Vec::with_capacity(size as usize);
        reader.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn get_version_chunk_stream(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let end = offset + size - 1;
        let range = format!("bytes={offset}-{end}");

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 get_version_chunk_stream failed: {}",
                    parse_s3_error(&e)
                ))
            })?;
        let stream = ByteStreamAdapter { inner: resp.body };

        Ok(Box::new(stream))
    }

    async fn list_version_chunks(
        &self,
        hash: &str,
        upload_id: &Option<String>,
    ) -> Result<Vec<CompleteFileChunk>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);
        let upload_id = upload_id
            .as_ref()
            .ok_or_else(|| OxenError::basic_str("S3 list_version_chunks missing upload_id"))?;

        let mut parts = Vec::new();
        let mut marker = None;

        loop {
            let resp = client
                .list_parts()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(upload_id)
                .set_part_number_marker(marker)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 list_version_chunks list_parts failed: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            if let Some(s3_parts) = resp.parts {
                for p in s3_parts {
                    let part_number = p.part_number.ok_or_else(|| {
                        OxenError::basic_str("S3 list_version_chunks missing part_number")
                    })?;

                    let etag = p.e_tag.ok_or_else(|| {
                        OxenError::basic_str("S3 list_version_chunks missing etag")
                    })?;
                    parts.push(CompleteFileChunk {
                        offset: None,
                        chunk_number: Some(part_number),
                        etag: Some(etag),
                    });
                }
            }

            if resp.is_truncated.unwrap_or(false) {
                marker = resp.next_part_number_marker;
            } else {
                break;
            }
        }

        Ok(parts)
    }

    async fn combine_version_chunks(
        &self,
        hash: &str,
        chunks: Option<Vec<CompleteFileChunk>>,
        upload_id: &Option<String>,
        _cleanup: bool,
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);
        let upload_id = upload_id
            .as_ref()
            .ok_or_else(|| OxenError::basic_str("S3 combine_version_chunks missing upload_id"))?;

        // Get list of chunks and sort them to ensure correct order
        let mut chunks = if let Some(c) = chunks {
            c
        } else {
            self.list_version_chunks(hash, &Some(upload_id.clone()))
                .await?
        };
        // Validate all chunks have chunk number and etag
        for chunk in &chunks {
            if chunk.chunk_number.is_none() || chunk.etag.is_none() {
                return Err(OxenError::basic_str(
                    "S3 combine_version_chunks missing chunk_number or etag",
                ));
            }
        }
        chunks.sort_by_key(|p| p.chunk_number.unwrap());

        let completed_parts: Vec<CompletedPart> = chunks
            .into_iter()
            .map(|chunk| {
                let part_number = chunk.chunk_number.unwrap();
                let etag = chunk.etag.unwrap();
                Ok(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build())
            })
            .collect::<Result<Vec<_>, OxenError>>()?;

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!(
                    "S3 complete_multipart_upload failed: {}",
                    parse_s3_error(&e)
                ))
            })?;

        Ok(())
    }

    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        match client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),

            Err(err) => Err(OxenError::basic_str(format!(
                "S3 version_exists failed: {}",
                parse_s3_error(&err)
            ))),
        }
    }

    async fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let version_dir = self.version_dir(hash);

        let mut continuation_token = None;
        loop {
            let res = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&version_dir)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 list_objects_v2 failed for prefix {version_dir}: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            let objects = res.contents();
            if objects.is_empty() {
                break;
            }

            let delete_objects = Delete::builder()
                .set_objects(Some(
                    objects
                        .iter()
                        .filter_map(|obj| {
                            let key = obj.key()?;
                            ObjectIdentifier::builder().key(key).build().ok()
                        })
                        .collect(),
                ))
                .build()
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 Delete builder failed for prefix {version_dir}: {e}"
                    ))
                })?;

            // TODO: Handle partial failure
            let delete_resp = client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete_objects)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 delete_objects failed for prefix {version_dir}: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            let errors = delete_resp.errors();
            if !errors.is_empty() {
                log::warn!(
                    "S3 delete_objects partial failure for prefix {}: {} objects failed",
                    version_dir,
                    errors.len()
                );
                for err in errors {
                    log::warn!(
                        "S3 delete_objects failed for key={:?}, code={:?}, message={:?}",
                        err.key(),
                        err.code(),
                        err.message(),
                    );
                }
            }
            continuation_token = res.next_continuation_token().map(|s| s.to_string());

            if continuation_token.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn delete_all_versions(&self) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let prefix = &self.prefix;
        let mut continuation_token = None;
        loop {
            let res = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 list_objects_v2 failed for prefix {prefix}: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            let objects = res.contents();
            if objects.is_empty() {
                break;
            }

            let delete_objects = Delete::builder()
                .set_objects(Some(
                    objects
                        .iter()
                        .filter_map(|obj| {
                            let key = obj.key()?;
                            ObjectIdentifier::builder().key(key).build().ok()
                        })
                        .collect(),
                ))
                .build()
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 Delete builder failed for prefix {prefix}: {e}"
                    ))
                })?;

            // TODO: Handle partial failure
            let delete_resp = client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete_objects)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 delete_objects failed for prefix {prefix}: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            let errors = delete_resp.errors();
            if !errors.is_empty() {
                log::warn!(
                    "S3 delete_all_versions partial failure for prefix {}: {} objects failed",
                    prefix,
                    errors.len()
                );
                for err in errors {
                    log::warn!(
                        "S3 delete_all_versions failed for key={:?}, code={:?}, message={:?}",
                        err.key(),
                        err.code(),
                        err.message(),
                    );
                }
            }
            continuation_token = res.next_continuation_token().map(|s| s.to_string());

            if continuation_token.is_none() {
                break;
            }
        }

        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        let client = self.init_client().await?;
        let mut versions = Vec::new();

        let prefix = &self.prefix;
        // '/' at the end to ensure we only list directories under the prefix "{namespace}/{repo_name}/"
        let root_prefix = format!("{prefix}/");
        let mut continuation_token = None;

        loop {
            let resp = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&root_prefix)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|e| {
                    OxenError::basic_str(format!(
                        "S3 list_objects_v2 failed for prefix {root_prefix}: {}",
                        parse_s3_error(&e)
                    ))
                })?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    if let Some(rest) = key.strip_prefix(&root_prefix) {
                        let mut parts = rest.split('/');
                        if let (Some(a), Some(b)) = (parts.next(), parts.next()) {
                            versions.push(format!("{a}{b}"));
                        }
                    }
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        versions.sort_unstable();
        versions.dedup();
        Ok(versions)
    }

    async fn clean_corrupted_versions(&self) -> Result<CleanCorruptedVersionsResult, OxenError> {
        // TODO: Implement S3 version chunk combination
        Err(OxenError::basic_str(
            "S3VersionStore clean_corrupted_versions not yet implemented",
        ))
    }

    fn storage_type(&self) -> &str {
        "s3"
    }

    fn storage_settings(&self) -> HashMap<String, String> {
        let mut settings = HashMap::new();
        settings.insert("bucket".to_string(), self.bucket.clone());
        settings.insert("prefix".to_string(), self.prefix.clone());
        settings
    }
}

use aws_sdk_s3::error::ProvideErrorMetadata;
use std::fmt::Debug;

pub fn parse_s3_error<E, R>(e: &SdkError<E, R>) -> String
where
    E: Debug + ProvideErrorMetadata,
    R: Debug,
{
    match e {
        SdkError::ServiceError(se) => {
            let meta = se.err().meta();
            let code = meta.code();
            let message = meta.message();

            format!("AWS ServiceError: code={code:?}, message={message:?}",)
        }
        SdkError::TimeoutError(te) => format!("AWS TimeoutError: {te:?}"),
        SdkError::DispatchFailure(df) => format!("AWS DispatchFailure: {df:?}"),
        SdkError::ConstructionFailure(cf) => format!("AWS ConstructionFailure: {cf:?}"),
        other => format!("Other AWS error: {other:?}"),
    }
}

struct ByteStreamAdapter {
    inner: ByteStream,
}

impl Stream for ByteStreamAdapter {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(e))) => {
                let err = io::Error::other(format!("{e}"));
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct S3VersionWriter {
    writer: tokio::io::DuplexStream,
    upload_handle: JoinHandle<Result<(), OxenError>>, // Background upload task
}

#[async_trait]
impl VersionWriter for S3VersionWriter {
    // Must be called specifically before the writer dropped
    async fn finish(mut self: Box<Self>) -> Result<(), OxenError> {
        // flush duplex
        self.writer.shutdown().await?;
        // await the upload response from S3
        self.upload_handle.await??;
        Ok(())
    }
}

impl AsyncWrite for S3VersionWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}
