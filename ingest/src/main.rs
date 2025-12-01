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
    // grab useful information from the event
    let headers = &event.headers();

    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"));

    // convert body to string
    let body_bytes = event.body();
    let body_str = std::str::from_utf8(body_bytes.as_ref()).map_err(|_| "Invalid UTF-8 in body")?;

    // handle conversion "overseeing" logic given the content type of the http request
    let normalized = match content_type.and_then(|s| s.to_str().ok()) {
        Some("text/plain") => handle_plaintext(&headers, &body_str),
        Some("application/json") => handle_json(&body_str),
        Some(other) => Err(format!("Unsupported: {}", other).into()),
        None => Err("Missing conten-type".into()),
    }?; // unwrap the result

    // serialize to json
    let message_json = serde_json::to_string(&normalized)?;

    // set up sqs integration from env vars
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sqs_client = aws_sdk_sqs::Client::new(&config);
    let queue_url = std::env::var("QUEUE_URL").expect("QUEUE_URL not set");

    // we send the serialized message to the broker
    sqs_client
        .send_message()
        .queue_url(queue_url)
        .message_body(message_json)
        .send()
        .await?;

    Ok(Response::builder()
        .status(202)
        .body(Body::from("Accepted"))?)
}

// borrow so we don't take ownership away from the lambda runtime
fn handle_json(body: &str) -> Result<NormalizedLog, Error> {
    let incoming: IncomingData = serde_json::from_str(&body)?;

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
        .ok_or("Missing X-Tenant-ID header")?; // converst Option to result, or returns error if None

    Ok(NormalizedLog {
        tenant_id: tenant_id.to_string(),
        text: body.to_string(),
        source: Some("plaintext".to_string()),
        timestamp: None,
        tags: None,
        metadata: None,
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
