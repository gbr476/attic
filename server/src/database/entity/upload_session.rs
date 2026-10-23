//! An unfinished chunked upload.

use sea_orm::entity::prelude::*;

pub type UploadSessionModel = Model;

/// An unfinished chunked upload.
///
/// The session owns a NAR in the `PendingUpload` state. Chunks are attached to
/// that NAR as they arrive, which protects them from garbage collection. When
/// the upload is finished, the NAR becomes valid and the session is deleted.
/// Deleting the NAR (e.g., when garbage collection reaps a stale upload)
/// deletes the session as well.
#[derive(Debug, Clone, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "upload_session")]
pub struct Model {
    /// Unique numeric ID of the session.
    #[sea_orm(primary_key)]
    pub id: i64,

    /// The secret token identifying the session.
    ///
    /// This is the `upload_id` handed to the client.
    #[sea_orm(unique, indexed)]
    pub token: String,

    /// ID of the pending NAR.
    #[sea_orm(indexed)]
    pub nar_id: i64,

    /// ID of the cache the path is uploaded to.
    pub cache_id: i64,

    /// The upload info submitted when the session was started, as JSON
    /// (`UploadPathNarInfo`).
    pub nar_info: String,

    /// The user who started the session.
    pub created_by: Option<String>,

    /// Timestamp when the session was started.
    pub created_at: ChronoDateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::nar::Entity",
        from = "Column::NarId",
        to = "super::nar::Column::Id"
    )]
    Nar,

    #[sea_orm(
        belongs_to = "super::cache::Entity",
        from = "Column::CacheId",
        to = "super::cache::Column::Id"
    )]
    Cache,
}

impl Related<super::nar::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Nar.def()
    }
}

impl Related<super::cache::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Cache.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
