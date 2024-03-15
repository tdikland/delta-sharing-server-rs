use uuid::Uuid;

use crate::catalog::{CatalogError, Pagination};

/// Cursor for paginating collections of sharable objects.
#[derive(Debug)]
pub struct PostgresCursor {
    last_seen_id: Option<Uuid>,
    limit: Option<u32>,
}

impl PostgresCursor {
    /// Create a new PostgresCursor.
    pub fn new(last_seen_id: Option<Uuid>, limit: Option<u32>) -> Self {
        Self {
            last_seen_id,
            limit,
        }
    }

    /// Return the last seen id.
    pub fn last_seen_id(&self) -> Uuid {
        match self.last_seen_id {
            Some(id) => id,
            None => Uuid::nil(),
        }
    }

    /// Return the limit.
    pub fn limit(&self) -> i32 {
        match self.limit {
            Some(lim) => lim as i32,
            None => 100,
        }
    }
}

impl Default for PostgresCursor {
    fn default() -> Self {
        Self {
            last_seen_id: None,
            limit: Some(500),
        }
    }
}

impl TryFrom<Pagination> for PostgresCursor {
    type Error = CatalogError;

    fn try_from(pagination: Pagination) -> Result<Self, Self::Error> {
        let last_seen_id = pagination
            .page_token()
            .map(|token| {
                Uuid::parse_str(token).map_err(|e| {
                    tracing::error!(pagination = ?pagination, error = ?e, "the pagination token could not be parsed as UUID");
                    CatalogError::malformed_pagination(
                        "the pagination token could not be parsed",
                    )
                })
            })
            .transpose()?;
        let pg_cursor = PostgresCursor::new(last_seen_id, pagination.max_results());
        Ok(pg_cursor)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::CatalogErrorKind;

    use super::*;

    #[test]
    fn parse_page_token_succes() {
        let pagination = Pagination::new(
            Some(42),
            Some(String::from("00000000-0000-0000-0000-000000000001")),
        );

        let cursor: PostgresCursor = pagination.try_into().unwrap();
        assert_eq!(cursor.last_seen_id(), Uuid::from_u128(1));
        assert_eq!(cursor.limit(), 42);
    }

    #[test]
    fn parse_page_token_failed() {
        let pagination = Pagination::new(Some(42), Some(String::from("invalid token")));

        let cursor: Result<PostgresCursor, CatalogError> = pagination.try_into();
        assert_eq!(
            cursor.unwrap_err().kind(),
            CatalogErrorKind::MalformedPagination
        );
    }
}
