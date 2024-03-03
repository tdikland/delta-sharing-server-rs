use sqlx::{postgres::PgRow, Row};

use crate::catalog::ShareInfo;

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
// pub struct Schema {
//     share: Share,
//     name: String,
//     id: Option<String>,
// }

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
// pub struct Table {
//     schema: Schema,
//     name: String,
//     id: Option<String>,
//     storage_path: String,
//     format: String,
// }

pub fn to_share_info(row: PgRow) -> Result<ShareInfo, sqlx::Error> {
    let share_id: String = row.try_get("share_id")?;
    let share_name: String = row.try_get("share_name")?;

    Ok(ShareInfo::new(share_name, Some(share_id)))
}

// impl TryFrom<PgRow> for Share {
//     type Error = sqlx::Error;

//     fn try_from(row: PgRow) -> Result<Self, Self::Error> {
//         let name: String = row.try_get("share_name")?;
//         let id: String = row.try_get("share_id")?;
//         Ok(ShareBuilder::new(name).id(id).build())
//     }
// }

// impl TryFrom<PgRow> for Schema {
//     type Error = sqlx::Error;

//     fn try_from(row: PgRow) -> Result<Self, Self::Error> {
//         let share_id: String = row.try_get("share_id")?;
//         let share_name: String = row.try_get("share_name")?;
//         let schema_id: String = row.try_get("schema_id")?;
//         let schema_name: String = row.try_get("schema_name")?;

//         let share = ShareBuilder::new(share_name).id(share_id).build();
//         let schema = SchemaBuilder::new(share, schema_name).id(schema_id).build();

//         Ok(schema)
//     }
// }

// impl TryFrom<PgRow> for Table {
//     type Error = sqlx::Error;

//     fn try_from(row: PgRow) -> Result<Self, Self::Error> {
//         let share_id: String = row.try_get("share_id")?;
//         let share_name: String = row.try_get("share_name")?;
//         let schema_id: String = row.try_get("schema_id")?;
//         let schema_name: String = row.try_get("schema_name")?;
//         let table_id: String = row.try_get("table_id")?;
//         let table_name: String = row.try_get("table_name")?;
//         let storage_path: String = row.try_get("storage_path")?;
//         let storage_format: Option<String> = row.try_get("storage_format")?;

//         let share = ShareBuilder::new(share_name).id(share_id).build();
//         let schema = SchemaBuilder::new(share, schema_name).id(schema_id).build();
//         let table = TableBuilder::new(schema, table_name, storage_path)
//             .id(table_id)
//             .set_format(storage_format)
//             .build();

//         Ok(table)
//     }
// }
