//! Types for share info request/response

use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// Type for pagination through a collection of shared objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListCursor {
    max_results: Option<u32>,
    page_token: Option<String>,
}

impl ListCursor {
    /// Create a new `ListCursor` with the given `max_results` and `page_token`
    pub fn new(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self {
            max_results,
            page_token,
        }
    }

    /// Retrieve the maximum amount of objects that should be fetched.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::share::ListCursor;
    ///
    /// let cursor = ListCursor::new(Some(3), None);
    /// assert_eq!(cursor.max_results(), Some(3));
    /// ```
    pub fn max_results(&self) -> Option<u32> {
        self.max_results
    }

    /// Retrieve the index from where to resume fetching.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::share::ListCursor;
    ///
    /// let cursor = ListCursor::new(None, Some(String::from("page1")));
    /// assert_eq!(cursor.page_token(), Some("page1"));
    /// ```
    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }

    /// Check whether or not this cursor is in the middle of fetching.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::share::ListCursor;
    ///
    /// let cursor = ListCursor::new(None, Some(String::from("page1")));
    /// assert!(cursor.has_page_token());
    ///
    /// let cursor = ListCursor::new(None, None);
    /// assert!(!cursor.has_page_token());
    /// ```
    pub fn has_page_token(&self) -> bool {
        self.page_token.is_some()
    }
}

/// Representation of a list that can be traversed using a [`ListCursor`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct List<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> List<T> {
    /// Create a new `List` of fetched objects with `items` and a `next_page_token`
    /// to resume fetching from the collection in a new request.
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }

    /// Add a new item to the `List`
    pub fn push(&mut self, item: T) {
        self.items.push(item);
    }

    /// Retrieve all items in the list.
    pub fn items(&self) -> &[T] {
        self.items.as_ref()
    }

    /// Retrieve the token that represents the cursor position.
    pub fn next_page_token(&self) -> Option<&String> {
        self.next_page_token.as_ref()
    }
}

impl<T> Deref for List<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
