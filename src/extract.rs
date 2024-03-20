use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::request::Parts};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{auth::RecipientId, catalog::Pagination, error::ServerError, reader::Version};

#[async_trait]
impl<S> FromRequestParts<S> for RecipientId
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let recipient_id = parts
            .extensions
            .get::<RecipientId>()
            .ok_or_else(|| {
                tracing::error!("the `RecepientId` extension was not set");
                ServerError::unauthorized("the `RecepientId` extension was not set")
            })
            .map(|x| x.clone())?;

        Ok(recipient_id)
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Pagination
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or_default();
        let value = serde_urlencoded::from_str(query)
            .map_err(|e| ServerError::invalid_query_params(e.to_string()))?;
        Ok(value)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct VersionQueryParams {
    starting_timestamp: Option<DateTime<Utc>>,
}

#[async_trait]
impl<S> FromRequestParts<S> for Version
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or_default();
        let value = serde_urlencoded::from_str::<VersionQueryParams>(query)
            .map_err(|_| ServerError::invalid_query_params("invalid version query parameter"))?;
        match value.starting_timestamp {
            Some(ts) => Ok(Version::Timestamp(ts)),
            None => Ok(Version::Latest),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
pub enum ResponseFormat {
    Parquet,
    Delta,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Capabilities {
    response_format: ResponseFormat,
    reader_features: Option<Vec<String>>,
}

impl Capabilities {
    pub fn response_format(&self) -> ResponseFormat {
        self.response_format
    }

    pub fn is_delta_format(&self) -> bool {
        self.response_format() == ResponseFormat::Delta
    }

    /// Returns the reader features if present.
    pub fn reader_features(&self) -> Option<&Vec<String>> {
        self.reader_features.as_ref()
    }

    /// Returns true if the reader features contain the given feature.
    pub fn has_reader_feature(&self, feature: &str) -> bool {
        self.reader_features()
            .map(|features| features.contains(&feature.to_owned()))
            .unwrap_or_default()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Capabilities
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let header = parts.headers.get("delta-sharing-capabilities");

        if let Some(h) = header {
            let value = h
                .to_str()
                .map_err(|_| ServerError::invalid_query_params("capability header"))?;
            let mut response_format = None;
            let mut reader_features = None;

            for pair in value.split(';') {
                let mut iter = pair.split('=');
                let key = iter.next().unwrap();
                let value = iter.next().unwrap_or_default();

                match key {
                    "responseformat" => {
                        response_format = {
                            match value {
                                "parquet" => Some(ResponseFormat::Parquet),
                                "delta" => Some(ResponseFormat::Delta),
                                _ => None,
                            }
                        }
                    }
                    "readerfeatures" => {
                        reader_features = Some(value.split(',').map(|s| s.to_owned()).collect())
                    }
                    _ => {}
                }
            }

            if let Some(response_format) = response_format {
                return Ok(Capabilities {
                    response_format,
                    reader_features,
                });
            }
        }

        Ok(Capabilities {
            response_format: ResponseFormat::Parquet,
            reader_features: None,
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TableDataParams {
    #[serde(default)]
    predicate_hints: Vec<String>,
    limit_hint: Option<i32>,
    version: Option<i32>,
    json_predicate_hints: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangePredicates {
    version_range: TableVersionRange,
    include_historical_metadata: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableVersionRange {
    Version {
        start: u64,
        end: u64,
    },
    Timestamp {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RawTableChangeParams {
    starting_version: Option<u64>,
    starting_timestamp: Option<String>,
    ending_version: Option<u64>,
    ending_timestamp: Option<String>,
    include_historical_metadata: Option<bool>,
}

#[async_trait]
impl<S> FromRequestParts<S> for TableChangePredicates
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or_default();
        let v = serde_urlencoded::from_str::<RawTableChangeParams>(query)
            .map_err(|_| ServerError::invalid_query_params(""))?;

        let range = match (
            v.starting_version,
            v.ending_version,
            v.starting_timestamp,
            v.ending_timestamp,
        ) {
            (Some(start), Some(end), None, None) => {
                if start > end {
                    return Err(ServerError::invalid_query_params(
                        "starting table version cannot be higher than ending table version",
                    ));
                }
                TableVersionRange::Version { start, end }
            }
            (None, None, Some(start), Some(end)) => {
                let start_ts = start
                    .parse::<DateTime<Utc>>()
                    .map_err(|e| ServerError::invalid_query_params(e.to_string()))?;
                let end_ts = end
                    .parse::<DateTime<Utc>>()
                    .map_err(|e| ServerError::invalid_query_params(e.to_string()))?;

                if end_ts < start_ts {
                    let msg = String::from(
                        "starting table timestamp must be before ending table timestamp",
                    );
                    return Err(ServerError::invalid_query_params(msg));
                }
                TableVersionRange::Timestamp {
                    start: start_ts,
                    end: end_ts,
                }
            }
            _ => {
                let msg = String::from("specify the range of table version either with `starting_version` and `ending_version` or `starting_timestamp` and `ending_timestamp`");
                return Err(ServerError::invalid_query_params(msg));
            }
        };

        Ok(TableChangePredicates {
            version_range: range,
            include_historical_metadata: v.include_historical_metadata.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::error::ServerErrorKind;

    use super::*;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::Request;
    use axum::Json;
    use chrono::TimeZone;
    use serde_json::json;

    #[tokio::test]
    async fn extract_recipient_id() {
        let req = Request::builder()
            .uri("http://example.com/test")
            .extension(RecipientId::known("foo"))
            .body(Body::empty())
            .unwrap();

        let recipient_id = RecipientId::from_request(req, &()).await.unwrap();
        assert_eq!(recipient_id.as_ref(), "foo");

        let req = Request::builder()
            .uri("http://example.com/test")
            .body(Body::empty())
            .unwrap();

        let err = RecipientId::from_request(req, &()).await.unwrap_err();
        assert_eq!(err.kind(), ServerErrorKind::Unauthorized);
    }

    #[tokio::test]
    async fn extract_pagination() {
        let uri = "http://example.com/test";
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(
            Pagination::from_request(req, &()).await.unwrap(),
            Pagination::new(None, None)
        );

        let uri = "http://example.com/test?maxResults=1";
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(
            Pagination::from_request(req, &()).await.unwrap(),
            Pagination::new(Some(1), None)
        );

        let uri = "http://example.com/test?pageToken=abcd";
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(
            Pagination::from_request(req, &()).await.unwrap(),
            Pagination::new(None, Some("abcd".to_owned()))
        );

        let uri = "http://example.com/test?maxResults=2&pageToken=efgh";
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(
            Pagination::from_request(req, &()).await.unwrap(),
            Pagination::new(Some(2), Some("efgh".to_owned()))
        );
    }

    #[tokio::test]
    async fn reject_pagination() {
        // Invalid datatype for maxResults -> should be number
        let uri = "http://example.com/test?maxResults=aaa";
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(
            Pagination::from_request(req, &()).await.unwrap_err(),
            ServerError::invalid_query_params("message")
        );
    }

    #[tokio::test]
    async fn extract_version() {
        let req = Request::builder()
            .uri("http://example.com/test")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Version::from_request(req, &()).await.unwrap(),
            Version::Latest
        );

        let req = Request::builder()
            .uri("http://example.com/test?startingTimestamp=2022-01-01T00:00:00Z")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Version::from_request(req, &()).await.unwrap(),
            Version::Timestamp(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn reject_version() {
        // Invalid type for startingTimestamp, should be in timestamp format
        let req = Request::builder()
            .uri("http://example.com/test?startingTimestamp=abc")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Version::from_request(req, &()).await.unwrap_err(),
            ServerError::invalid_query_params("message")
        );
    }

    #[tokio::test]
    async fn extract_capabilities() {
        // Default capabilities
        let req = Request::builder()
            .uri("http://example.com/test")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Capabilities::from_request(req, &()).await.unwrap(),
            Capabilities {
                response_format: ResponseFormat::Parquet,
                reader_features: None
            }
        );

        // Custom parquet capabilities
        let req = Request::builder()
            .uri("http://example.com/test")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .header("delta-sharing-capabilities", "responseformat=parquet")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Capabilities::from_request(req, &()).await.unwrap(),
            Capabilities {
                response_format: ResponseFormat::Parquet,
                reader_features: None
            }
        );

        // Custom delta capabilities
        let req = Request::builder()
            .uri("http://example.com/test")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .header(
                "delta-sharing-capabilities",
                "responseformat=delta;readerfeatures=deletionvectors,columnmapping",
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Capabilities::from_request(req, &()).await.unwrap(),
            Capabilities {
                response_format: ResponseFormat::Delta,
                reader_features: Some(vec![
                    "deletionvectors".to_owned(),
                    "columnmapping".to_owned()
                ])
            }
        );
    }

    #[tokio::test]
    async fn extract_table_data_params() {
        let params = json!({
            "predicateHints": [],
            "limitHint": 1000,
            "version": 2,
            "jsonPredicateHints": ""
        });
        let req = Request::builder()
            .uri("http://example.com/test")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .body(Body::from(serde_json::to_string(&params).unwrap()))
            .unwrap();

        assert_eq!(
            Json::<TableDataParams>::from_request(req, &())
                .await
                .unwrap()
                .deref(),
            &TableDataParams {
                predicate_hints: vec![],
                limit_hint: Some(1000),
                version: Some(2),
                json_predicate_hints: Some(String::new())
            }
        );

        let req = Request::builder()
            .uri("http://example.com/test?startingTimestamp=2000-01-01T00:00:00Z")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            Version::from_request(req, &()).await.unwrap(),
            Version::Timestamp(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn extract_table_change_params() {
        let req = Request::builder()
            .uri("http://example.com/test?startingVersion=0&endingVersion=2")
            .body(Body::empty())
            .unwrap();

        assert_eq!(
            TableChangePredicates::from_request(req, &()).await.unwrap(),
            TableChangePredicates {
                version_range: TableVersionRange::Version { start: 0, end: 2 },
                include_historical_metadata: false
            }
        );

        let req = Request::builder()
        .uri("http://example.com/test?startingTimestamp=2000-01-01T00:00:00Z&endingTimestamp=2000-01-02T00:00:00Z")
        .body(Body::empty())
        .unwrap();

        assert_eq!(
            TableChangePredicates::from_request(req, &()).await.unwrap(),
            TableChangePredicates {
                version_range: TableVersionRange::Timestamp {
                    start: Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
                    end: Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
                },
                include_historical_metadata: false
            }
        );
    }
}
