use base64::{engine::general_purpose, Engine};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// A struct to represent the primary key of the DynamoDB table
#[derive(Serialize, Deserialize)]
struct DynamoKey {
    pk: String,
    sk: String,
}

/// Convert a pagination token to a DynamoDB key
fn token_to_key(token: &str) -> HashMap<String, AttributeValue> {
    let decoded_token = general_purpose::URL_SAFE.decode(token).unwrap();
    let key: DynamoKey = serde_json::from_slice(&decoded_token).unwrap();
    let map = HashMap::from_iter([
        (String::from("PK"), AttributeValue::S(key.pk)),
        (String::from("SK"), AttributeValue::S(key.sk)),
    ]);
    map
}

/// Convert a DynamoDB key to a pagination token
fn key_to_token(key: &HashMap<String, AttributeValue>) -> String {
    let dynamo_key = DynamoKey {
        pk: key.get("PK").unwrap().as_s().unwrap().to_owned(),
        sk: key.get("SK").unwrap().as_s().unwrap().to_owned(),
    };
    let json = serde_json::to_vec(&dynamo_key).unwrap();
    general_purpose::URL_SAFE.encode(&json)
}
