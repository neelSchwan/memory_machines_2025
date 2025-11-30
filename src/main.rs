use std::collections::HashMap;

use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_lambda_events::apigw::ApiGatewayV2httpRequest;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde::Serialize;
use serde_json::{Value, json};

/*
    we have two possibilties for input:
        txt (so header is plain/text): we treat the body as raw text, and grab tenant from header
        json (so header is application/json):we grab tenant from the header, and parse into the struct.
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let func = service_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}

// function recieves an event with a firstname field and returns a message to the caller
async fn func(event: LambdaEvent<ApiGatewayV2httpRequest>) -> Result<NormalizedLog, Error> {
    let headers = event.payload.headers;
    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"));

    match content_type.map(|s| s.as_str()) {
        Some("text/plain") => handle_plaintext(),
        Some("application/json") => handle_json(),
        Some(other) => return error(format!("Unsupported content type: {}", other)),
        None => return error("Missing content-type header"),
    }
    Ok(())
}
