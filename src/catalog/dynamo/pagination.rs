use base64::{engine::general_purpose, Engine};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use aws_sdk_dynamodb::{operation::query::builders::QueryFluentBuilder, types::AttributeValue};

use crate::catalog::Pagination;

/// A struct to represent the primary key of the DynamoDB table
#[derive(Serialize, Deserialize)]
struct DynamoKey {
    pk: String,
    sk: String,
}

/// Convert a pagination token to a DynamoDB key
pub fn token_to_key(token: &str) -> HashMap<String, AttributeValue> {
    let decoded_token = general_purpose::URL_SAFE.decode(token).unwrap();
    let key: DynamoKey = serde_json::from_slice(&decoded_token).unwrap();
    
    HashMap::from_iter([
        (String::from("PK"), AttributeValue::S(key.pk)),
        (String::from("SK"), AttributeValue::S(key.sk)),
    ])
}

/// Convert a DynamoDB key to a pagination token
pub fn key_to_token(key: &HashMap<String, AttributeValue>) -> String {
    let dynamo_key = DynamoKey {
        pk: key.get("PK").unwrap().as_s().unwrap().to_owned(),
        sk: key.get("SK").unwrap().as_s().unwrap().to_owned(),
    };
    let json = serde_json::to_vec(&dynamo_key).unwrap();
    general_purpose::URL_SAFE.encode(json)
}

pub trait PaginationExt {
    fn with_pagination(self, pagination: &Pagination) -> Self;
}

impl PaginationExt for QueryFluentBuilder {
    fn with_pagination(self, pagination: &Pagination) -> Self {
        let mut builder = self;
        if let Some(max_results) = pagination.max_results() {
            builder = builder.limit(max_results as i32);
        }
        if let Some(page_token) = pagination.page_token() {
            builder = builder.set_exclusive_start_key(Some(token_to_key(page_token)));
        }
        builder
    }
}
