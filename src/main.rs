use std::collections::HashMap;

// use aws_config::BehaviorVersion;
// use aws_config::meta::region::RegionProviderChain;
use aws_lambda_events::apigw::ApiGatewayV2httpRequest;
use lambda_http::{Error, http::HeaderMap};
use lambda_runtime::{LambdaEvent, service_fn};
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
    let func = service_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}

// function recieves an event with a firstname field and returns a message to the caller
async fn func(event: LambdaEvent<ApiGatewayV2httpRequest>) -> Result<NormalizedLog, Error> {
    // grab useful information from the event
    let headers = &event.payload.headers;
    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"));
    let body_str = event.payload.body.as_ref().ok_or("Missing body")?;

    // handle conversion "overseeing" logic given the content type of the http request
    let normalized = match content_type.and_then(|s| s.to_str().ok()) {
        Some("text/plain") => handle_plaintext(&headers, &body_str),
        Some("application/json") => handle_json(&body_str),
        Some(other) => Err(format!("Unsupported: {}", other).into()),
        None => Err("Missing conten-type".into()),
    }?; // unwrap the result

    Ok(normalized)
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
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        let body = r#"{"tenant_id":"test","log_id":"123","text":"hello"}"#.to_string();

        let result = handle_json(&body).unwrap();
        assert_eq!(result.tenant_id, "test");
    }
}
