use sea_orm_migration::prelude::*;

use crate::database::entity::cache;
use crate::database::entity::nar;
use crate::database::entity::upload_session::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20261023_000001_add_upload_session_table"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Entity)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Column::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Column::Token).string().not_null())
                    .col(ColumnDef::new(Column::NarId).big_integer().not_null())
                    .col(ColumnDef::new(Column::CacheId).big_integer().not_null())
                    .col(ColumnDef::new(Column::NarInfo).string().not_null())
                    .col(ColumnDef::new(Column::CreatedBy).string().null())
                    .col(
                        ColumnDef::new(Column::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKeyCreateStatement::new()
                            .name("fk_upload_session_nar")
                            .from_tbl(Entity)
                            .from_col(Column::NarId)
                            .to_tbl(nar::Entity)
                            .to_col(nar::Column::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKeyCreateStatement::new()
                            .name("fk_upload_session_cache")
                            .from_tbl(Entity)
                            .from_col(Column::CacheId)
                            .to_tbl(cache::Entity)
                            .to_col(cache::Column::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-upload-session-token")
                    .table(Entity)
                    .col(Column::Token)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-upload-session-nar-id")
                    .table(Entity)
                    .col(Column::NarId)
                    .to_owned(),
            )
            .await
    }
}
