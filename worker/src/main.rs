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
    tenant_id: String, // DynamoDB Partition Key
    log_id: String, // DynamoDB Sort Key
    source: String, // e.g. "json", "plaintext"
    original_text: String, // The unmodified text from the log
    modified_data: String, // Text with phone numbers redacted
    processed_at: String, // ISO8601 timestamp of when we processed it
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("WORKER: starting up runtime");
    let result = run(service_fn(handler)).await;
    println!("WORKER: shutting down runtime (result: {:?})", result);
    result
}

/// this is called by AWS Lambda runtime when SQS messages arrive.
/// extract the SQS records from the event
/// iterate and process each record in the batch
/// (Lambda runtime will auto-delete messages from SQS on success)
async fn handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    println!("HANDLER: received {} SQS record(s)", event.payload.records.len());
    for (i, record) in event.payload.records.into_iter().enumerate() {
        println!("HANDLER: processing record #{i}"
    );
        // get body, skip if missing
        let body = match record.body {
            Some(b) => {
                println!("HANDLER: record #{i} has body length {}", b.len());
                b
            }

            None => {
                eprintln!("SQS record missing body");
                continue;
            }
        };

        // deserialize, skip if invalid json
        let log = match serde_json::from_str::<NormalizedLog>(&body) {
            Ok(l) => {
                println!(
                    "HANDLER: record #{i} deserialized successfully (tenant_id = {})",
                    l.tenant_id
                );
                l
            }
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

/// for the log, we first grab length, and sleep for simulated time
async fn process_message(log: NormalizedLog) -> Result<(), Error> {
    println!(
        "PROCESS: start tenant={} text_len={}",
        log.tenant_id,
        log.text.len()
    );

    // first sleep
    simulate_heavy_processing(&log.text).await;
    println!("PROCESS: finished simulated processing");

    // redact the numbers like we see in the example
    let modified_data = redact_phone_numbers(&log.text);
    println!(
        "PROCESS: redacted text from '{}' -> '{}'",
        log.text, modified_data
    );

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

    println!(
        "PROCESS: prepared ProcessedLog for tenant={} log_id={}",
        processed_log.tenant_id, processed_log.log_id
    );

    // try to write to DynamoDB
    println!("PROCESS: saving to DynamoDB...");
    match save_to_dynamodb(processed_log).await {
        Ok(_) => println!("PROCESS: DynamoDB write successful"),
        Err(e) => eprintln!("PROCESS: DynamoDB write FAILED: {:?}", e),
    }

    Ok(())
}

/// Count the characters in the text
/// Calculate sleep duration: char_count * 50 milliseconds
/// Use tokio::time::sleep() for async non-blocking sleep
async fn simulate_heavy_processing(text: &str) {
    let char_count = text.chars().count() as u64;
    let sleep_ms = char_count * 50;
    println!(
        "SIMULATE: sleeping for {} ms ({} chars)",
        sleep_ms, char_count
    );
    sleep(Duration::from_millis(sleep_ms)).await;
}

// parse text, find phone numbers via regex (?)
// replace that index in the text with "[REDACTED]"
fn redact_phone_numbers(text: &str) -> String {
    let re = Regex::new(r"\b(?:\d{3}-\d{4}|\d{3}-\d{3}-\d{4})\b").unwrap();
    let result = re.replace_all(text, "[REDACTED]").to_string();
    println!("REDACT: before='{}' after='{}'", text, result);
    result
}

/// save processed log to DynamoDB with tenant isolation
///
/// DynamoDB Schema:
/// - Partition Key: tenant_id (String)
/// - Sort Key: log_id (String)
async fn save_to_dynamodb(log: ProcessedLog) -> Result<(), Error> {
    // Get table name from environment variable
    let table_name = std::env::var("TABLE_NAME")
        .unwrap_or_else(|_| "processed_logs".to_string());
    println!("DDB: using table '{table_name}'");
    // init AWS config and DynamoDB client
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    println!("DDB: AWS config loaded");

    let client = aws_sdk_dynamodb::Client::new(&config);
    println!("DDB: client initialized");

    // build the item as a hashmap of AttributeValues
    let mut item = HashMap::new();
    item.insert(
        "tenant_id".to_string(),
        AttributeValue::S(log.tenant_id),
    );
    item.insert(
        "log_id".to_string(),
        AttributeValue::S(log.log_id),
    );
    item.insert(
        "source".to_string(),
        AttributeValue::S(log.source),
    );
    item.insert(
        "original_text".to_string(),
        AttributeValue::S(log.original_text),
    );
    item.insert(
        "modified_data".to_string(),
        AttributeValue::S(log.modified_data),
    );
    item.insert(
        "processed_at".to_string(),
        AttributeValue::S(log.processed_at),
    );

    // do the put item operation
    match client
        .put_item()
        .table_name(table_name)
        .set_item(Some(item))
        .send()
        .await
    {
        Ok(_) => {
            println!("DDB: PutItem succeeded");
            Ok(())
        }
        Err(e) => {
            eprintln!("DDB: PutItem error: {:?}", e);
            Err(Box::from(format!("Failed to save to DynamoDB: {}", e)) as Box<
                dyn std::error::Error + Send + Sync,
            >)
        }
    }
}
