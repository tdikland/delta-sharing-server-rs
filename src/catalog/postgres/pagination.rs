use uuid::Uuid;

use crate::catalog::Pagination;

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
    type Error = &'static str;
    fn try_from(cursor: Pagination) -> Result<Self, Self::Error> {
        let last_seen_id = cursor
            .page_token()
            .map(|token| Uuid::parse_str(token).map_err(|_| "invalid page token"))
            .transpose()?;
        let pg_cursor = PostgresCursor::new(last_seen_id, cursor.max_results());
        Ok(pg_cursor)
    }
}
