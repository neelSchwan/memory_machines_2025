use std::{collections::HashMap};

use aws_lambda_events::event::sqs::SqsEvent;
use tokio::time::{sleep, Duration};
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::Utc;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// NormalizedLog: Matches the structure sent by the ingest lambda
/// This is the format of messages we receive from SQS
#[derive(Debug, Clone)]
#[derive(Deserialize)]
struct NormalizedLog {
    tenant_id: String,
    text: String,
    source: Option<String>,
    timestamp: Option<String>,
    tags: Option<Vec<String>>,
    metadata: Option<HashMap<String, String>>,
}

/// ProcessedLog: The structure we store in DynamoDB
/// Represents the final processed log with redactions and metadata
#[derive(Debug, Clone)]
#[derive(Serialize)]
struct ProcessedLog {
    tenant_id: String,     // DynamoDB Partition Key
    log_id: String,        // DynamoDB Sort Key
    source: String,        // e.g. "json", "plaintext"
    original_text: String, // The unmodified text from the log
    modified_data: String, // Text with phone numbers redacted
    processed_at: String,  // ISO8601 timestamp of when we processed it
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(handler)).await
}

/// This is called by AWS Lambda runtime when SQS messages arrive.
/// Extract the SQS records from the event
/// Iterate and process each record in the batch
/// (Lambda runtime will auto-delete messages from SQS on success)
async fn handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    for record in event.payload.records {
        // Get body, skip if missing
        let body = match record.body {
            Some(b) => b,
            None => {
                eprintln!("SQS record missing body");
                continue;
            }
        };

        // deserialize, skip if invalid json
        let log = match serde_json::from_str::<NormalizedLog>(&body) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error deserializing message {:?}", e);
                continue;
            }
        };

        // process, log error but continue the batch
        if let Err(e) = process_message(log).await {
            eprintln!("Error processing message: {:?}", e)
        }
    }
    Ok(())
}

// for the log, we first grab length, and sleep for simulated time
// 
async fn process_message(log: NormalizedLog) -> Result<(), Error> {
    // first sleep
    simulate_heavy_processing(&log.text).await;

    // redact the numbers like we see in the example
    let modified_data = redact_phone_numbers(&log.text);

    let log_id = log.metadata
        .as_ref()
        .and_then(|m| m.get("log_id"))
        .cloned()
        .unwrap_or_else(|| Uuid::nil().to_string());
    
    let processed_log = ProcessedLog {
        tenant_id: log.tenant_id,
        log_id,
        source: log.source.unwrap_or_else(|| "unknown".to_string()),
        original_text: log.text,
        modified_data,
        processed_at: Utc::now().to_rfc3339(),
    };

    save_to_dynamodb(processed_log).await?;

    Ok(())
}

/// Count the characters in the text
/// Calculate sleep duration: char_count * 50 milliseconds
/// Use tokio::time::sleep() for async non-blocking sleep
async fn simulate_heavy_processing(text: &str) {
    let char_count = text.chars().count() as u64;
    let sleep_ms = char_count * 50;
    sleep(Duration::from_millis(sleep_ms)).await;
}

// parse text, find phone numbers via regex (?)
// replace that index in the text with "[REDACTED]"
fn redact_phone_numbers(text: &str) -> String {
    // simple phone pattern like 555-1234 or 555-555-1234
    let re = Regex::new(r"\b(?:\d{3}-\d{4}|\d{3}-\d{3}-\d{4})\b").unwrap();
    // find number
    return re.replace_all(text, "[REDACTED]").to_string();
}
async fn save_to_dynamodb(_log: ProcessedLog) -> Result<(), Error> {
    // TODO: implement real DynamoDB persistence; this is a compile-time stub for now
    println!("Saving processed log id: {}", _log.log_id);
    Ok(())
}
