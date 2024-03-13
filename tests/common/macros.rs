#[macro_export]
macro_rules! assert_response_snapshot {
    ($value:expr) => {
        with_settings!(
            { filters => vec![("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "[id]")] },
            { assert_snapshot!($value) }
        )
    };
}
