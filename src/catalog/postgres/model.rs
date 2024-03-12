use sqlx::FromRow;
use uuid::Uuid;

use crate::catalog::{Schema, Share, ShareReaderError, Table};

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

impl ShareInfoModel {
    pub fn try_into_share(self) -> Result<Share, ShareReaderError> {
        Share::builder()
            .id(self.id.to_string())
            .name(self.name)
            .build()
    }
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

impl SchemaInfoModel {
    pub fn try_into_schema(self) -> Result<Schema, ShareReaderError> {
        Schema::builder()
            .id(self.id.to_string())
            .name(self.name)
            .share_name(self.share_name)
            .build()
    }
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

impl TableInfoModel {
    pub fn try_into_table(self) -> Result<Table, ShareReaderError> {
        Table::builder()
            .id(self.id.to_string())
            .share_id(self.share_id.to_string())
            .name(self.name)
            .schema_name(self.schema_name)
            .share_name(self.share_name)
            .storage_path(self.storage_path)
            .build()
    }
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
