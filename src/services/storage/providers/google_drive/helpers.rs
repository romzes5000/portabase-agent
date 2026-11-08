use crate::services::storage::providers::google_drive::models::GoogleDriveProviderConfig;
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, RefreshToken, TokenResponse, TokenUrl, basic::BasicClient,
    reqwest::Client as OAuth2ReqwestClient,
};
use reqwest::{Client as ReqwestClient, StatusCode};
use reqwest::{Client, header};
use serde_json::{Value, json};

pub const DRIVE_FILES_URL: &str =
    "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true";
pub const DRIVE_FILES_LIST_URL: &str = "https://www.googleapis.com/drive/v3/files";
pub const DRIVE_RESUMABLE_UPLOAD_URL: &str =
    "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriveFolderHit {
    pub id: String,
    pub created_time: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AfterCreateChoice {
    pub use_id: String,
    pub trash_own_if_empty: Option<String>,
}

pub fn pick_oldest_folder(folders: &[DriveFolderHit]) -> Option<&DriveFolderHit> {
    folders.iter().min_by(|a, b| {
        a.created_time
            .cmp(&b.created_time)
            .then_with(|| a.id.cmp(&b.id))
    })
}

pub fn parse_folder_list(status: u16, body: &Value) -> Result<Vec<DriveFolderHit>> {
    if !(200..300).contains(&status) {
        return Err(anyhow!("list folders failed: HTTP {status}"));
    }
    let Some(files) = body.get("files").and_then(Value::as_array) else {
        return Err(anyhow!("list folders failed: missing files"));
    };
    Ok(files
        .iter()
        .filter_map(|file| {
            let id = file.get("id")?.as_str()?.to_string();
            let created_time = file
                .get("createdTime")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Some(DriveFolderHit { id, created_time })
        })
        .collect())
}

pub fn after_create_choice(listed: &[DriveFolderHit], created_id: &str) -> AfterCreateChoice {
    match pick_oldest_folder(listed) {
        Some(oldest) if oldest.id != created_id => AfterCreateChoice {
            use_id: oldest.id.clone(),
            trash_own_if_empty: Some(created_id.to_string()),
        },
        Some(oldest) => AfterCreateChoice {
            use_id: oldest.id.clone(),
            trash_own_if_empty: None,
        },
        None => AfterCreateChoice {
            use_id: created_id.to_string(),
            trash_own_if_empty: None,
        },
    }
}

pub const QUOTA_RETRY_MAX: u32 = 5;

pub fn is_retryable_drive_quota(status: u16, body: &str) -> bool {
    if status == 429 {
        return true;
    }
    if status != 403 {
        return false;
    }
    let body = body.to_ascii_lowercase();
    body.contains("ratelimitexceeded")
        || body.contains("userratelimitexceeded")
        || body.contains("quotaexceeded")
}

pub fn quota_backoff_secs(attempt: u32) -> u64 {
    5 * (1u64 << attempt.min(4))
}

pub async fn retry_drive_response<F, Fut>(send: F) -> Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    retry_drive_response_with_backoff(send, quota_backoff_secs).await
}

pub async fn retry_drive_response_with_backoff<F, Fut, B>(
    mut send: F,
    backoff_secs: B,
) -> Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    B: Fn(u32) -> u64,
{
    let mut retries = 0u32;
    loop {
        let res = match send().await {
            Ok(res) => res,
            Err(e) if e.is_timeout() || e.is_connect() => {
                if retries >= QUOTA_RETRY_MAX {
                    return Err(e).context("Too many retries");
                }
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(retries + 1))).await;
                retries += 1;
                continue;
            }
            Err(e) => return Err(e).context("drive request"),
        };
        let status = res.status();
        if status.is_success() || status == StatusCode::PERMANENT_REDIRECT {
            return Ok(res);
        }
        let code = status.as_u16();
        let text = res.text().await.unwrap_or_default();
        if is_retryable_drive_quota(code, &text) && retries < QUOTA_RETRY_MAX {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs(retries))).await;
            retries += 1;
            continue;
        }
        return Err(anyhow!("Drive HTTP {code}: {text}"));
    }
}

pub fn append_from_pending(buf: &mut Vec<u8>, pending: &mut Bytes, remaining: &mut u64) {
    if pending.is_empty() || *remaining == 0 {
        return;
    }
    let to_take = (*remaining as usize).min(pending.len());
    buf.extend_from_slice(&pending[..to_take]);
    *pending = pending.slice(to_take..);
    *remaining -= to_take as u64;
}

pub fn collect_windows(chunks: Vec<Bytes>, total: u64, window: u64) -> Result<Vec<Vec<u8>>> {
    let mut pending = Bytes::new();
    let mut iter = chunks.into_iter();
    let mut uploaded = 0u64;
    let mut windows = Vec::new();

    while uploaded < total {
        let chunk_size = (total - uploaded).min(window);
        let mut buf = Vec::with_capacity(chunk_size as usize);
        let mut remaining = chunk_size;

        while remaining > 0 {
            if pending.is_empty() {
                match iter.next() {
                    Some(next) => pending = next,
                    None => {
                        if uploaded + buf.len() as u64 != total {
                            return Err(anyhow!("Stream ended early"));
                        }
                        break;
                    }
                }
            }
            append_from_pending(&mut buf, &mut pending, &mut remaining);
        }

        if buf.is_empty() && uploaded < total {
            return Err(anyhow!("Unexpected end of stream"));
        }
        uploaded += buf.len() as u64;
        windows.push(buf);
    }

    Ok(windows)
}

pub async fn get_google_drive_token(config: &GoogleDriveProviderConfig) -> Result<String> {
    let http_client = OAuth2ReqwestClient::new();

    let oauth_client = BasicClient::new(ClientId::new(config.client_id.clone()))
        .set_client_secret(ClientSecret::new(config.client_secret.clone()))
        .set_auth_uri(
            AuthUrl::new("https://accounts.google.com/o/oauth2/auth".to_string())
                .context("invalid auth uri")?,
        )
        .set_token_uri(
            TokenUrl::new("https://oauth2.googleapis.com/token".to_string())
                .context("invalid token uri")?,
        );

    let token_result = oauth_client
        .exchange_refresh_token(&RefreshToken::new(config.refresh_token.clone()))
        .request_async(&http_client)
        .await
        .context("failed to exchange refresh token")?;

    Ok(token_result.access_token().secret().clone())
}

async fn list_named_folders(
    client: &ReqwestClient,
    token: &str,
    parent_id: &str,
    name: &str,
) -> Result<Vec<DriveFolderHit>> {
    let query = format!(
        "'{parent_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    );

    let res = retry_drive_response(|| {
        client
            .get(DRIVE_FILES_LIST_URL)
            .bearer_auth(token)
            .query(&[
                ("q", query.as_str()),
                ("fields", "files(id,name,createdTime)"),
                ("orderBy", "createdTime"),
                ("pageSize", "100"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
                ("corpora", "allDrives"),
            ])
            .send()
    })
    .await
    .context("list folders failed")?;

    let status = res.status().as_u16();
    let body = res.json::<Value>().await.context("list folders json")?;
    parse_folder_list(status, &body)
}

async fn folder_has_children(client: &ReqwestClient, token: &str, folder_id: &str) -> Result<bool> {
    let query = format!("'{folder_id}' in parents and trashed=false");
    let res = retry_drive_response(|| {
        client
            .get(DRIVE_FILES_LIST_URL)
            .bearer_auth(token)
            .query(&[
                ("q", query.as_str()),
                ("fields", "files(id)"),
                ("pageSize", "1"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
                ("corpora", "allDrives"),
            ])
            .send()
    })
    .await
    .context("list folder children failed")?;

    let status = res.status().as_u16();
    let body = res
        .json::<Value>()
        .await
        .context("list folder children json")?;
    let files = parse_folder_list(status, &body)?;
    Ok(!files.is_empty())
}

async fn trash_own_folder(client: &ReqwestClient, token: &str, folder_id: &str) -> Result<()> {
    retry_drive_response(|| {
        client
            .patch(format!(
                "{DRIVE_FILES_LIST_URL}/{folder_id}?supportsAllDrives=true"
            ))
            .bearer_auth(token)
            .json(&json!({ "trashed": true }))
            .send()
    })
    .await
    .context("trash folder failed")?;
    Ok(())
}

pub async fn ensure_folder_path(
    config: &GoogleDriveProviderConfig,
    path_parts: &[&str],
) -> Result<String> {
    if path_parts.is_empty() {
        return Ok(config.folder_id.clone());
    }

    let token = get_google_drive_token(config).await?;
    let client = ReqwestClient::new();
    let mut parent_id = config.folder_id.clone();

    for &name in path_parts {
        let existing = list_named_folders(&client, &token, &parent_id, name).await?;
        if let Some(oldest) = pick_oldest_folder(&existing) {
            parent_id = oldest.id.clone();
            continue;
        }

        let create_payload = json!({
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        });

        let created = retry_drive_response(|| {
            client
                .post(DRIVE_FILES_URL)
                .bearer_auth(&token)
                .json(&create_payload)
                .send()
        })
        .await
        .context("create folder failed")?;

        let folder = created
            .json::<Value>()
            .await
            .context("create folder json")?;
        let created_id = folder["id"]
            .as_str()
            .ok_or_else(|| anyhow!("No id returned after folder creation"))?
            .to_string();

        let listed_again = list_named_folders(&client, &token, &parent_id, name).await?;
        let choice = after_create_choice(&listed_again, &created_id);
        if let Some(own_id) = choice.trash_own_if_empty.as_deref() {
            if !folder_has_children(&client, &token, own_id).await? {
                trash_own_folder(&client, &token, own_id).await?;
            }
        }
        parent_id = choice.use_id;
    }

    Ok(parent_id)
}

pub async fn find_file_by_name(
    config: &GoogleDriveProviderConfig,
    file_name: &str,
    folder_id: &str,
) -> Result<Option<String>> {
    let token = get_google_drive_token(config).await?;
    let client = ReqwestClient::new();

    let query = format!("'{folder_id}' in parents and name='{file_name}' and trashed=false");

    let res = retry_drive_response(|| {
        client
            .get(DRIVE_FILES_LIST_URL)
            .bearer_auth(&token)
            .query(&[
                ("q", query.as_str()),
                ("fields", "files(id,name,createdTime)"),
                ("orderBy", "createdTime"),
                ("pageSize", "100"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
                ("corpora", "allDrives"),
            ])
            .send()
    })
    .await
    .context("find file failed")?;

    let status = res.status().as_u16();
    let body = res.json::<Value>().await.context("find file json")?;
    let hits = parse_folder_list(status, &body)?;
    Ok(pick_oldest_folder(&hits).map(|hit| hit.id.clone()))
}

pub async fn upload_stream_to_google_drive(
    config: &GoogleDriveProviderConfig,
    full_path: &str,
    mut content_stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
    total_size: u64,
    mime_type: Option<&str>,
) -> Result<()> {
    let path_parts: Vec<&str> = full_path.split('/').filter(|s| !s.is_empty()).collect();
    if path_parts.is_empty() {
        return Err(anyhow::anyhow!("Invalid path: empty"));
    }

    let file_name = *path_parts.last().unwrap();
    let folder_path = &path_parts[..path_parts.len() - 1];

    let folder_id = ensure_folder_path(config, folder_path).await?;

    if find_file_by_name(config, file_name, &folder_id)
        .await?
        .is_some()
    {
        return Err(anyhow::anyhow!("File already exists: {}", full_path));
    }

    let token = get_google_drive_token(config).await?;
    let client = Client::new();

    let mime = mime_type.unwrap_or("application/octet-stream");

    let metadata = json!({
        "name": file_name,
        "parents": [folder_id],
        "mimeType": mime,
    });

    let session_res = retry_drive_response(|| {
        client
            .post(DRIVE_RESUMABLE_UPLOAD_URL)
            .bearer_auth(&token)
            .header("X-Upload-Content-Type", mime)
            .header("X-Upload-Content-Length", total_size.to_string()) // Helps a lot
            .json(&metadata)
            .send()
    })
    .await
    .context("Failed to initiate resumable upload")?;

    let upload_url = session_res
        .headers()
        .get(header::LOCATION)
        .ok_or_else(|| anyhow::anyhow!("No Location header"))?
        .to_str()?
        .to_string();

    const CHUNK_SIZE: u64 = 8 * 1024 * 1024;

    let mut uploaded: u64 = 0;
    let mut pending = Bytes::new();

    while uploaded < total_size {
        let chunk_size = (total_size - uploaded).min(CHUNK_SIZE);

        let mut chunk_bytes = Vec::with_capacity(chunk_size as usize);
        let mut remaining = chunk_size;

        while remaining > 0 {
            if pending.is_empty() {
                match content_stream.next().await {
                    Some(Ok(bytes)) => pending = bytes,
                    Some(Err(e)) => return Err(e).context("Stream error during chunk"),
                    None => {
                        if uploaded + chunk_bytes.len() as u64 != total_size {
                            return Err(anyhow::anyhow!("Stream ended early"));
                        }
                        break;
                    }
                }
            }
            append_from_pending(&mut chunk_bytes, &mut pending, &mut remaining);
        }

        if chunk_bytes.is_empty() && uploaded < total_size {
            return Err(anyhow::anyhow!("Unexpected end of stream"));
        }

        let range_end = uploaded + chunk_bytes.len() as u64 - 1;
        let content_range = if uploaded + chunk_bytes.len() as u64 == total_size {
            format!("bytes {}-{}/{}", uploaded, range_end, total_size)
        } else {
            format!("bytes {}-{}/*", uploaded, range_end)
        };

        retry_drive_response(|| {
            client
                .put(&upload_url)
                .header("Content-Range", &content_range)
                .header("Content-Length", chunk_bytes.len().to_string())
                .body(chunk_bytes.clone())
                .send()
        })
        .await
        .context("Chunk upload failed")?;
        uploaded += chunk_bytes.len() as u64;
        tracing::info!("Uploaded {}/{} bytes", uploaded, total_size);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sixteen_mib_chunk_survives_eight_mib_windows() {
        let window = 8 * 1024 * 1024;
        let payload = vec![7u8; 16 * 1024 * 1024];
        let windows = collect_windows(
            vec![Bytes::from(payload.clone())],
            payload.len() as u64,
            window,
        )
        .expect("windows");

        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].len(), window as usize);
        assert_eq!(windows[1].len(), window as usize);
        let mut joined = windows[0].clone();
        joined.extend_from_slice(&windows[1]);
        assert_eq!(joined, payload);
    }

    #[test]
    fn drive_write_urls_carry_supports_all_drives() {
        assert!(DRIVE_RESUMABLE_UPLOAD_URL.contains("supportsAllDrives=true"));
        assert!(DRIVE_RESUMABLE_UPLOAD_URL.contains("uploadType=resumable"));
        assert!(DRIVE_FILES_URL.contains("supportsAllDrives=true"));
    }

    fn hit(id: &str, created_time: &str) -> DriveFolderHit {
        DriveFolderHit {
            id: id.to_string(),
            created_time: created_time.to_string(),
        }
    }

    #[test]
    fn pick_oldest_folder_single() {
        let folders = [hit("a", "2026-11-06T12:33:28.307Z")];
        assert_eq!(pick_oldest_folder(&folders).unwrap().id, "a");
    }

    #[test]
    fn pick_oldest_folder_two_created_times() {
        let folders = [
            hit("new", "2026-11-08T03:00:22.325Z"),
            hit("old", "2026-11-06T12:33:28.307Z"),
        ];
        assert_eq!(pick_oldest_folder(&folders).unwrap().id, "old");
    }

    #[test]
    fn pick_oldest_folder_tie_breaks_on_id() {
        let folders = [
            hit("z-new", "2026-11-06T12:33:28.307Z"),
            hit("a-old", "2026-11-06T12:33:28.307Z"),
        ];
        assert_eq!(pick_oldest_folder(&folders).unwrap().id, "a-old");
    }

    #[test]
    fn parse_folder_list_http_error_does_not_look_empty() {
        let body = serde_json::json!({"error": {"code": 403, "message": "rateLimitExceeded"}});
        let err = parse_folder_list(403, &body).unwrap_err().to_string();
        assert!(err.contains("HTTP 403"), "{err}");
    }

    #[test]
    fn parse_folder_list_200_without_files_is_error() {
        let body = serde_json::json!({"error": {"code": 403}});
        let err = parse_folder_list(200, &body).unwrap_err().to_string();
        assert!(err.contains("missing files"), "{err}");
    }

    #[test]
    fn parse_folder_list_reads_created_time() {
        let body = serde_json::json!({
            "files": [
                {"id": "new", "createdTime": "2026-11-08T03:00:22.325Z"},
                {"id": "old", "createdTime": "2026-11-06T12:33:28.307Z"}
            ]
        });
        let hits = parse_folder_list(200, &body).unwrap();
        assert_eq!(pick_oldest_folder(&hits).unwrap().id, "old");
    }

    #[test]
    fn after_create_trashes_own_empty_when_older_exists() {
        let listed = [
            hit("old", "2026-11-06T12:33:28.307Z"),
            hit("mine", "2026-11-08T03:00:22.325Z"),
        ];
        assert_eq!(
            after_create_choice(&listed, "mine"),
            AfterCreateChoice {
                use_id: "old".to_string(),
                trash_own_if_empty: Some("mine".to_string()),
            }
        );
    }

    #[test]
    fn after_create_keeps_own_when_list_still_empty() {
        assert_eq!(
            after_create_choice(&[], "mine"),
            AfterCreateChoice {
                use_id: "mine".to_string(),
                trash_own_if_empty: None,
            }
        );
    }

    #[test]
    fn after_create_keeps_own_when_it_is_oldest() {
        let listed = [hit("mine", "2026-11-06T12:33:28.307Z")];
        assert_eq!(
            after_create_choice(&listed, "mine"),
            AfterCreateChoice {
                use_id: "mine".to_string(),
                trash_own_if_empty: None,
            }
        );
    }

    #[test]
    fn quota_429_is_retryable() {
        assert!(is_retryable_drive_quota(429, ""));
    }

    #[test]
    fn quota_403_rate_limit_is_retryable() {
        let body = r#"{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}"#;
        assert!(is_retryable_drive_quota(403, body));
    }

    #[test]
    fn quota_403_user_and_quota_reasons_are_retryable() {
        assert!(is_retryable_drive_quota(
            403,
            r#"{"error":{"errors":[{"reason":"userRateLimitExceeded"}]}}"#
        ));
        assert!(is_retryable_drive_quota(403, "QuotaExceeded"));
    }

    #[test]
    fn other_403_is_not_retryable() {
        let body = r#"{"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}"#;
        assert!(!is_retryable_drive_quota(403, body));
    }

    #[test]
    fn quota_backoff_doubles_from_five_seconds() {
        assert_eq!(quota_backoff_secs(0), 5);
        assert_eq!(quota_backoff_secs(1), 10);
        assert_eq!(quota_backoff_secs(2), 20);
        assert_eq!(quota_backoff_secs(4), 80);
        assert_eq!(quota_backoff_secs(8), 80);
    }

    #[tokio::test]
    async fn retries_403_quota_then_succeeds() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string(r#"{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}"#),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"ok":true}"#))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/files", server.uri());
        let resp = retry_drive_response_with_backoff(|| client.get(&url).send(), |_| 0)
            .await
            .expect("retry");
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn other_403_fails_without_retry() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403).set_body_string(
                r#"{"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}"#,
            ))
            .expect(1)
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/files", server.uri());
        let err = retry_drive_response_with_backoff(|| client.get(&url).send(), |_| 0)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Drive HTTP 403"), "{err}");
        assert!(err.contains("insufficientFilePermissions"), "{err}");
    }
}
