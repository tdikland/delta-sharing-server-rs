use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct Client {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct Share {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct ShareAcl {
    pub id: Uuid,
    pub share_id: Uuid,
    pub client_id: Uuid,
}
