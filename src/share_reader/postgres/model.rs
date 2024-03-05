use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct ClientModel {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct ShareModel {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct ShareInfoModel {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct SchemaModel {
    pub id: Uuid,
    pub name: String,
    pub share_id: Uuid,
}

#[derive(Debug, FromRow)]
pub struct SchemaInfoModel {
    pub id: Uuid,
    pub name: String,
    pub share_name: String,
}

#[derive(Debug, FromRow)]
pub struct TableModel {
    pub id: Uuid,
    pub name: String,
    pub schema_id: Uuid,
    pub storage_path: String,
}

#[derive(Debug, FromRow)]
pub struct TableInfoModel {
    pub id: Uuid,
    pub share_id: Uuid,
    pub name: String,
    pub schema_name: String,
    pub share_name: String,
    pub storage_path: String,
}

#[derive(Debug, FromRow)]
pub struct ShareAclModel {
    pub id: Uuid,
    pub share_id: Uuid,
    pub client_id: Uuid,
}

#[derive(Debug, FromRow)]
pub struct SchemaAclModel {
    pub id: Uuid,
    pub schema_id: Uuid,
    pub client_id: Uuid,
}

#[derive(Debug, FromRow)]
pub struct TableAclModel {
    pub id: Uuid,
    pub table_id: Uuid,
    pub client_id: Uuid,
}
