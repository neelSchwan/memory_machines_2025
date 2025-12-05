use std::collections::HashMap;

use lambda_http::{Body, Error, Response, http::HeaderMap, service_fn};
use serde::{Deserialize, Serialize};
/*
    we have two possibilties for input:
        txt (so header is plain/text): we treat the body as raw text, and grab tenant from header
        json (so header is application/json):we grab tenant from the body, and parse into the struct.
*/
#[derive(Serialize)]
struct NormalizedLog {
    tenant_id: String,
    text: String,
    source: Option<String>,    // e.g. "web", "mobile", "plaintext"
    timestamp: Option<String>, // may be client provided
    tags: Option<Vec<String>>,
    metadata: Option<HashMap<String, String>>,
}
#[derive(Deserialize)]
struct IncomingData {
    tenant_id: String,
    log_id: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_http::run(service_fn(func)).await
}

// function recieves an event with a firstname field and returns a message to the caller
async fn func(event: lambda_http::Request) -> Result<Response<Body>, Error> {
    println!("INGEST: received request");

    // grab useful information from the event
    let headers = &event.headers();

    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"));

    // convert body to string
    let body_bytes = event.body();
    let body_str = std::str::from_utf8(body_bytes.as_ref())
        .map_err(|e| {
            eprintln!("INGEST ERROR: Invalid UTF-8 in body: {}", e);
            "Invalid UTF-8 in body"
        })?;

    println!("INGEST: body length = {} bytes", body_str.len());

    // handle conversion "overseeing" logic given the content type of the http request
    let normalized = match content_type.and_then(|s| s.to_str().ok()) {
        Some("text/plain") => {
            println!("INGEST: handling text/plain");
            handle_plaintext(&headers, &body_str)
        }
        Some("application/json") => {
            println!("INGEST: handling application/json");
            handle_json(&body_str)
        }
        Some(other) => {
            eprintln!("INGEST ERROR: Unsupported content-type: {}", other);
            Err(format!("Unsupported content-type: {}", other).into())
        }
        None => {
            eprintln!("INGEST ERROR: Missing Content-Type header");
            Err("Missing Content-Type header".into())
        }
    }?; // unwrap the result

    println!("INGEST: normalized tenant_id={}", normalized.tenant_id);

    // serialize to json
    let message_json = serde_json::to_string(&normalized)
        .map_err(|e| {
            eprintln!("INGEST ERROR: Failed to serialize: {}", e);
            e
        })?;

    // set up sqs integration from env vars
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    let queue_url = std::env::var("QUEUE_URL").expect("QUEUE_URL not set");

    // we send the serialized message to the broker
    println!("INGEST: sending to SQS queue");
    sqs_client
        .send_message()
        .queue_url(queue_url)
        .message_body(message_json)
        .send()
        .await
        .map_err(|e| {
            eprintln!("INGEST ERROR: SQS send failed: {}", e);
            format!("Failed to send to SQS: {}", e)
        })?;

    println!("INGEST: successfully queued message");

    Ok(Response::builder()
        .status(202)
        .body(Body::from("Accepted (202)"))?)
}

// borrow so we don't take ownership away from the lambda runtime
fn handle_json(body: &str) -> Result<NormalizedLog, Error> {
    let incoming: IncomingData = serde_json::from_str(&body)
        .map_err(|e| {
            eprintln!("INGEST ERROR: Failed to parse JSON body: {}", e);
            e
        })?;

    println!("INGEST: parsed JSON tenant_id={} log_id={}", incoming.tenant_id, incoming.log_id);

    let mut metadata = HashMap::new();
    metadata.insert("log_id".to_string(), incoming.log_id);

    Ok(NormalizedLog {
        tenant_id: incoming.tenant_id,
        text: incoming.text,
        source: Some("json".to_string()),
        timestamp: None,
        tags: None,
        metadata: Some(metadata),
    })
}

fn handle_plaintext(headers: &HeaderMap, body: &str) -> Result<NormalizedLog, Error> {
    let tenant_id = headers
        .get("X-Tenant-ID")
        .and_then(|v| v.to_str().ok()) // if header exists and is valid UTF-8
        .ok_or_else(|| {
            eprintln!("INGEST ERROR: Missing X-Tenant-ID header for plaintext request");
            "Missing X-Tenant-ID header"
        })?; // converst Option to result, or returns error if None

    let log_id = uuid::Uuid::new_v4().to_string();
    println!("INGEST: parsed plaintext tenant_id={} generated_log_id={}", tenant_id, log_id);

    let mut metadata = HashMap::new();
    metadata.insert("log_id".to_string(), log_id);

    Ok(NormalizedLog {
        tenant_id: tenant_id.to_string(),
        text: body.to_string(),
        source: Some("plaintext".to_string()),
        timestamp: None,
        tags: None,
        metadata: Some(metadata),
    })
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use lambda_http::http::HeaderMap;

    #[test]
    fn test_handle_json() {
        let body = r#"{"tenant_id":"test","log_id":"123","text":"hello"}"#.to_string();

        let result = handle_json(&body).unwrap();
        assert_eq!(result.tenant_id, "test");
    }

    #[test]
    fn test_handle_plaintext() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Tenant-ID", "test".parse().unwrap());

        let body = "abcdefghijklmnop".to_string();
        let result = handle_plaintext(&headers, &body).unwrap();

        assert_eq!(result.tenant_id, "test");
    }
}
