use aws_sdk_dynamodb::{
    operation::query::builders::QueryFluentBuilder,
    types::{AttributeValue, ConditionCheck},
};

use crate::auth::ClientId;

use super::{convert, DynamoCatalogConfig};

pub trait ConditionExt {
    fn shares_for_client_cond(self, client_id: &ClientId, config: &DynamoCatalogConfig) -> Self;

    fn schemas_for_client_share_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self;

    fn tables_for_client_share_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self;

    fn tables_for_client_schema_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self;
}

impl ConditionExt for QueryFluentBuilder {
    fn shares_for_client_cond(self, client_id: &ClientId, config: &DynamoCatalogConfig) -> Self {
        self.expression_attribute_names("#PK", config.client_id())
            .expression_attribute_names("#SK", config.securable())
            .expression_attribute_values(":pk", AttributeValue::S(client_id.to_string()))
            .expression_attribute_values(":sk", AttributeValue::S("SHARE".to_owned()))
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)")
    }

    fn schemas_for_client_share_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self {
        self.expression_attribute_names("#PK", config.client_id())
            .expression_attribute_names("#SK", config.securable())
            .expression_attribute_values(":pk", AttributeValue::S(client_id.to_string()))
            .expression_attribute_values(
                ":sk",
                AttributeValue::S(format!("SCHEMA#{}.", share_name)),
            )
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)")
    }

    fn tables_for_client_share_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self {
        self.expression_attribute_names("#PK", config.client_id())
            .expression_attribute_names("#SK", config.securable())
            .expression_attribute_values(":pk", AttributeValue::S(client_id.to_string()))
            .expression_attribute_values(":sk", AttributeValue::S(format!("TABLE#{}.", share_name)))
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)")
    }

    fn tables_for_client_schema_cond(
        self,
        client_id: &ClientId,
        share_name: &str,
        schema_name: &str,
        config: &DynamoCatalogConfig,
    ) -> Self {
        self.expression_attribute_names("#PK", config.client_id())
            .expression_attribute_names("#SK", config.securable())
            .expression_attribute_values(":pk", AttributeValue::S(client_id.to_string()))
            .expression_attribute_values(
                ":sk",
                AttributeValue::S(format!("TABLE#{}.{}.", share_name, schema_name)),
            )
            .key_condition_expression("#PK = :pk AND begins_with(#SK, :sk)")
    }
}

pub fn share_exists_check(
    client_id: &ClientId,
    share_name: &str,
    config: &DynamoCatalogConfig,
) -> ConditionCheck {
    let key = convert::to_share_key(client_id, share_name, config);
    ConditionCheck::builder()
        .table_name(config.table_name())
        .set_key(Some(key))
        .expression_attribute_names("#PK", config.client_id())
        .expression_attribute_names("#SK", config.securable())
        .condition_expression("attribute_exists(#PK) AND attribute_exists(#SK)")
        .build()
        .unwrap()
}
