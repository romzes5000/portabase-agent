use crate::services::api::ApiClient;
use crate::services::api::client::{derive_agent_key, sign_request};

use base64::{Engine as _, engine::general_purpose};
use reqwest::Method;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const AGENT_ID: &str = "11111111-1111-4111-8111-111111111111";

fn vector_master_key() -> Vec<u8> {
    (0u8..32).collect()
}

#[test]
fn derived_agent_key_matches_the_dashboard_vector() {
    let key = derive_agent_key(&vector_master_key(), AGENT_ID);

    assert_eq!(
        hex::encode(key),
        "16bee8dcf9d5f497cdd882ca208ff7f630ca02a5ecfe157f34239838fdcd9006"
    );
}

#[test]
fn request_signature_matches_the_dashboard_vector() {
    let key = derive_agent_key(&vector_master_key(), AGENT_ID);
    let path = format!("/api/agent/{AGENT_ID}/status");

    assert_eq!(
        sign_request(
            &key,
            "POST",
            &path,
            "1760968800",
            br#"{"version":"1.24.0","databases":[]}"#
        ),
        "ad1e56a50ac0c4a3f539f3180d2957ebeb9a61bd1df44e57b50f8e59dd269069"
    );
    assert_eq!(
        sign_request(
            &key,
            "POST",
            &path,
            "1760968800",
            "{\"name\":\"Base é\"}".as_bytes()
        ),
        "d3e6911c9273e0224b8dbae72cabdd3720973f1866c651fb4c99e1ac03e73231"
    );
}

#[test]
fn invalid_master_key_logs_an_error_before_panicking() {
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();

    let result = tracing::subscriber::with_default(subscriber, || {
        std::panic::catch_unwind(|| {
            ApiClient::new("http://dashboard").with_agent_auth(AGENT_ID, "not base64!")
        })
    });

    assert!(result.is_err(), "an invalid master key must stop the agent");
    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("ERROR") && output.contains("Invalid EDGE_KEY"),
        "logs: {output}"
    );
}

#[tokio::test]
async fn every_request_is_signed_with_a_fresh_timestamp() {
    let server = MockServer::start().await;
    let route = format!("/agent/{AGENT_ID}/status");
    Mock::given(method("POST"))
        .and(path(route.as_str()))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let client = ApiClient::new(server.uri()).with_agent_auth(
        AGENT_ID,
        &general_purpose::STANDARD.encode(vector_master_key()),
    );
    let body = serde_json::json!({"version": "1.24.0", "databases": [{"name": "Base é"}]});

    for i in 0..2 {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
        let _: Option<serde_json::Value> = client
            .request_with_body(Method::POST, &route, &body)
            .await
            .expect("signed request accepted by the mock");
    }

    let key = derive_agent_key(&vector_master_key(), AGENT_ID);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let requests = server.received_requests().await.unwrap();
    let mut timestamps = Vec::new();
    for req in &requests {
        let ts = req
            .headers
            .get("x-agent-timestamp")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ts.chars().all(|c| c.is_ascii_digit()), "timestamp {ts}");
        assert!(now.abs_diff(ts.parse::<u64>().unwrap()) <= 5);
        assert_eq!(req.headers.get("content-type").unwrap(), "application/json");
        assert_eq!(
            req.headers
                .get("x-agent-signature")
                .unwrap()
                .to_str()
                .unwrap(),
            sign_request(&key, "POST", &format!("/api{route}"), ts, &req.body)
        );
        timestamps.push(ts.to_string());
    }
    assert_eq!(requests.len(), 2);
    assert_ne!(
        timestamps[0], timestamps[1],
        "each call signs with its own timestamp"
    );
}
