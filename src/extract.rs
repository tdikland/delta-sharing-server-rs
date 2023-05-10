use std::ops::Deref;

use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::request::Parts, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    error::ServerError,
    protocol::{share::ListCursor, table::Version},
};

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Pagination(pub ListCursor);

#[async_trait]
impl<S> FromRequestParts<S> for Pagination
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or_default();
        let value = serde_urlencoded::from_str(query).map_err(|e| {
            ServerError::InvalidPaginationParameters {
                reason: e.to_string(),
            }
        })?;
        Ok(Self(value))
    }
}

impl Deref for Pagination {
    type Target = ListCursor;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableVersion {
    Latest,
    Timestamp(DateTime<Utc>),
}

impl TableVersion {
    pub fn into_version(self) -> Version {
        match self {
            Self::Latest => Version::Latest,
            Self::Timestamp(ts) => Version::Timestamp(ts),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTableVersion {
    starting_timestamp: Option<DateTime<Utc>>,
}

#[async_trait]
impl<S> FromRequestParts<S> for TableVersion
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or_default();
        let value = serde_urlencoded::from_str::<RawTableVersion>(query)
            .map_err(|_| ServerError::InvalidTableStartingTimestamp)?;
        match value.starting_timestamp {
            Some(ts) => Ok(TableVersion::Timestamp(ts)),
            None => Ok(TableVersion::Latest),
        }
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

pub type TableDataPredicates = Json<TableDataParams>;

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
            .map_err(|_| ServerError::InvalidTableStartingTimestamp)?;

        let range = match (
            v.starting_version,
            v.ending_version,
            v.starting_timestamp,
            v.ending_timestamp,
        ) {
            (Some(start), Some(end), None, None) => {
                if start > end {
                    return Err(ServerError::InvalidTableVersionRange {
                        reason: "starting table version cannot be higher than ending table version"
                            .to_string(),
                    });
                }
                TableVersionRange::Version { start, end }
            }
            (None, None, Some(start), Some(end)) => {
                let start_ts = start.parse::<DateTime<Utc>>().map_err(|e| {
                    ServerError::InvalidTableVersionRange {
                        reason: e.to_string(),
                    }
                })?;
                let end_ts = end.parse::<DateTime<Utc>>().map_err(|e| {
                    ServerError::InvalidTableVersionRange {
                        reason: e.to_string(),
                    }
                })?;

                if end_ts < start_ts {
                    let msg = String::from(
                        "starting table timestamp must be before ending table timestamp",
                    );
                    return Err(ServerError::InvalidTableVersionRange { reason: msg });
                }
                TableVersionRange::Timestamp {
                    start: start_ts,
                    end: end_ts,
                }
            }
            _ => {
                let msg = String::from("specify the range of table version either with `starting_version` and `ending_version` or `starting_timestamp` and `ending_timestamp`");
                return Err(ServerError::InvalidTableVersionRange { reason: msg });
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
    use super::*;
    use axum::extract::FromRequest;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::Request;
    use chrono::TimeZone;
    use serde_json::json;

    async fn check_pagination_ok(route: impl AsRef<str>, value: ListCursor) {
        let uri = format!("http://example.com{}", route.as_ref());
        let req = Request::builder().uri(&uri).body(()).unwrap();
        assert_eq!(Pagination::from_request(req, &()).await.unwrap().0, value);
    }

    async fn check_pagination_err(route: impl AsRef<str>, err: ServerError) {
        let uri = format!("http://example.com{}", route.as_ref());
        let req = Request::builder().uri(&uri).body(()).unwrap();
        assert_eq!(Pagination::from_request(req, &()).await.unwrap_err(), err);
    }

    #[tokio::test]
    async fn extract_pagination() {
        let exp = ListCursor::new(None, None);
        check_pagination_ok("/test", exp).await;

        let exp = ListCursor::new(Some(1), None);
        check_pagination_ok("/test?maxResults=1", exp).await;

        let exp = ListCursor::new(None, Some("abcd".to_owned()));
        check_pagination_ok("/test?pageToken=abcd", exp).await;

        let exp = ListCursor::new(Some(2), Some("efgh".to_owned()));
        check_pagination_ok("/test?maxResults=2&pageToken=efgh", exp).await;
    }

    #[tokio::test]
    async fn reject_invalid_pagination() {
        // Invalid datatype for maxResults -> should be number
        let exp = ServerError::InvalidPaginationParameters {
            reason: "invalid digit found in string".to_owned(),
        };
        check_pagination_err("/test?maxResults=aaa", exp).await;
    }

    #[tokio::test]
    async fn extract_table_starting_version() {
        let req = Request::builder()
            .uri("http://example.com/test")
            .body(())
            .unwrap();
        assert_eq!(
            TableVersion::from_request(req, &()).await.unwrap(),
            TableVersion::Latest
        );

        let req = Request::builder()
            .uri("http://example.com/test?startingTimestamp=2000-01-01T00:00:00Z")
            .body(())
            .unwrap();
        assert_eq!(
            TableVersion::from_request(req, &()).await.unwrap(),
            TableVersion::Timestamp(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn reject_invalid_table_starting_version() {
        // Cannot parse timestamp
        let req = Request::builder()
            .uri("http://example.com/test?startingTimestamp=abc")
            .body(())
            .unwrap();
        assert_eq!(
            TableVersion::from_request(req, &()).await.unwrap_err(),
            ServerError::InvalidTableStartingTimestamp
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
            .body(serde_json::to_string(&params).unwrap())
            .unwrap();

        assert_eq!(
            TableDataPredicates::from_request(req, &())
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
            .body(())
            .unwrap();
        assert_eq!(
            TableVersion::from_request(req, &()).await.unwrap(),
            TableVersion::Timestamp(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn extract_table_change_params() {
        let req = Request::builder()
            .uri("http://example.com/test?startingVersion=0&endingVersion=2")
            .body(())
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
        .body(())
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
