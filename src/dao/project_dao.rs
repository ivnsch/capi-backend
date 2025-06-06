use std::sync::Arc;

use algonaut::transaction::contract_account::ContractAccount;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use core_::flows::create_project::model::{CreateProjectSpecs, CreateSharesSpecs, Project};
use data_encoding::BASE64;
use tokio_postgres::Client;
use uuid::Uuid;

use super::db::{get_address, get_bytes, get_microalgos, get_u64};

#[async_trait]
pub trait ProjectDao: Sync + Send {
    async fn init(&self) -> Result<()>;

    async fn save_project(&self, project: &Project) -> Result<String>;
    async fn load_project(&self, id: i32) -> Result<Project>;
    async fn load_project_with_uuid(&self, uuid: &Uuid) -> Result<Project>;
}
pub struct ProjectDaoImpl {
    pub client: Arc<Client>,
}

#[async_trait]
impl ProjectDao for ProjectDaoImpl {
    async fn init(&self) -> Result<()> {
        let _ = self
            .client
            .execute(
                "CREATE TABLE IF NOT EXISTS project(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            creator TEXT NOT NULL,
            asset_price TEXT NOT NULL,
            token_name TEXT NOT NULL,
            share_count TEXT NOT NULL,
            investors_share TEXT NOT NULL,
            share_id TEXT NOT NULL,
            app_id TEXT NOT NULL,
            invest_b TEXT NOT NULL,
            staking_b TEXT NOT NULL,
            central_b TEXT NOT NULL,
            customer_b TEXT NOT NULL,
            uuid TEXT NOT NULL
        );",
                &[],
            )
            .await?;
        // note: execute returns "rows modified", for create table it's always 0
        Ok(())
    }

    async fn save_project(&self, project: &Project) -> Result<String> {
        let id_rows = self.client
            .query(
                "INSERT INTO project (name, creator, asset_price, token_name, share_count, investors_share, share_id, app_id, invest_b, staking_b, central_b, customer_b, uuid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id;",
                &[
                    &project.specs.name,
                    &project.creator.to_string(),
                    &project.specs.asset_price.0.to_string(),
                    &project.specs.shares.token_name.to_string(),
                    &project.specs.shares.count.to_string(),
                    &project.specs.investors_share.to_string(),
                    &project.shares_asset_id.to_string(),
                    &project.central_app_id.to_string(),
                    &BASE64.encode(&project.invest_escrow.program.0),
                    &BASE64.encode(&project.staking_escrow.program.0),
                    &BASE64.encode(&project.central_escrow.program.0),
                    &BASE64.encode(&project.customer_escrow.program.0),
                    &project.uuid.to_string(),
                ],
            )
            .await?;

        let id_row = match id_rows.as_slice() {
            [row] => row,
            _ => return Err(anyhow!("Unexpected row count: {}", id_rows.len())),
        };
        let id: i32 = id_row.get(0);
        let id_str = id.to_string();

        log::debug!("Saved project, row id: {}", id_str);

        Ok(id_str)
    }

    async fn load_project(&self, id: i32) -> Result<Project> {
        let project_rows = self.client.query(
            "SELECT name, asset_price, token_name, share_count, investors_share, creator, share_id, app_id, invest_b, staking_b, central_b, customer_b, uuid FROM project WHERE id=$1;", 
            &[&id]).await?;

        let project_row = match project_rows.as_slice() {
            [row] => row,
            _ => return Err(anyhow!("Project not found: {}", id)),
        };

        Ok(Project {
            specs: CreateProjectSpecs {
                name: project_row.get(0),
                asset_price: get_microalgos(project_row, 1)?,
                shares: CreateSharesSpecs {
                    token_name: project_row.get(2),
                    count: get_u64(project_row, 3)?,
                },
                investors_share: get_u64(project_row, 4)?,
            },
            creator: get_address(project_row, 5)?,
            shares_asset_id: get_u64(project_row, 6)?,
            central_app_id: get_u64(project_row, 7)?,
            invest_escrow: ContractAccount::new(get_bytes(project_row, 8)?),
            staking_escrow: ContractAccount::new(get_bytes(project_row, 9)?),
            central_escrow: ContractAccount::new(get_bytes(project_row, 10)?),
            customer_escrow: ContractAccount::new(get_bytes(project_row, 11)?),
            uuid: project_row.get::<_, String>(12).parse()?,
        })
    }

    // copy of load_project that queries with uuid - not refactoring yet as we'll remove load_project soon likely
    async fn load_project_with_uuid(&self, uuid: &Uuid) -> Result<Project> {
        let project_rows = self.client.query(
            "SELECT name, asset_price, token_name, share_count, investors_share, creator, share_id, app_id, invest_b, staking_b, central_b, customer_b, uuid FROM project WHERE uuid=$1;", 
            &[&uuid.to_string()]).await?;

        let project_row = match project_rows.as_slice() {
            [row] => row,
            _ => return Err(anyhow!("Project not found for uuid: {}", uuid)),
        };

        Ok(Project {
            specs: CreateProjectSpecs {
                name: project_row.get(0),
                asset_price: get_microalgos(project_row, 1)?,
                shares: CreateSharesSpecs {
                    token_name: project_row.get(2),
                    count: get_u64(project_row, 3)?,
                },
                investors_share: get_u64(project_row, 4)?,
            },
            creator: get_address(project_row, 5)?,
            shares_asset_id: get_u64(project_row, 6)?,
            central_app_id: get_u64(project_row, 7)?,
            invest_escrow: ContractAccount::new(get_bytes(project_row, 8)?),
            staking_escrow: ContractAccount::new(get_bytes(project_row, 9)?),
            central_escrow: ContractAccount::new(get_bytes(project_row, 10)?),
            customer_escrow: ContractAccount::new(get_bytes(project_row, 11)?),
            uuid: project_row.get::<_, String>(12).parse()?,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{convert::TryInto, sync::Arc};

    use super::{ProjectDao, ProjectDaoImpl};
    use crate::{dao::db::create_db_client, logger::init_logger};
    use anyhow::{Error, Result};
    use core_::api::json_workaround::ProjectJson;
    use tokio::test;

    #[test]
    #[ignore]
    async fn test_create_table() -> Result<()> {
        init_logger();
        let project_dao = create_test_project_dao().await?;

        project_dao.init().await?;
        Ok(())
    }

    // to be executed after test_create_table
    #[test]
    #[ignore]
    async fn test_insert_and_load_a_project() -> Result<()> {
        init_logger();
        let project_dao = create_test_project_dao().await?;

        // insert
        // generated with client app - convenience to test quickly, should be replaced with regular mock data
        let json = r#"{"specs":{"name":"my1project","shares":{"token_name":"foo","count":100},"investors_share":40,"asset_price":1000000},"creator_address":"MKRBTLNZRS3UZZDS5OWPLP7YPHUDNKXFUFN5PNCJ3P2XRG74HNOGY6XOYQ","shares_asset_id":42,"central_app_id":50,"invest_escrow":{"address":"SV2LIUFR5AL2BZOMGW3SAYU5FT2T662NOXPVKXF3GKGTDYRZJMHENNZS2Y","program":[4,32,6,6,42,0,232,7,43,4,50,4,34,18,51,2,17,35,18,16,51,3,17,33,4,18,16,64,0,9,50,4,34,18,64,0,83,36,67,51,2,17,35,18,51,2,16,33,5,18,16,51,2,18,36,18,16,51,2,1,37,14,16,51,2,32,50,3,18,16,51,2,21,50,3,18,16,51,3,17,33,4,18,16,51,3,16,33,5,18,16,51,3,18,36,18,16,51,3,1,37,14,16,51,3,32,50,3,18,16,51,3,21,50,3,18,16,66,0,91,51,0,16,34,18,51,3,17,35,18,16,51,3,20,128,32,247,10,15,104,164,223,249,27,116,139,66,224,167,91,33,215,215,35,34,187,44,221,159,36,227,39,167,77,162,152,169,0,18,16,51,3,1,37,14,16,51,3,21,50,3,18,16,51,3,32,50,3,18,16,51,1,8,51,3,18,129,192,132,61,11,18,16,51,3,18,51,4,18,18,16]},"staking_escrow":{"address":"64FA62FE374RW5ELILQKOWZB27LSGIV3FTOZ6JHDE6TU3IUYVEAKZXC3DQ","program":[4,32,6,4,6,0,42,43,232,7,50,4,35,18,51,0,17,37,18,16,51,1,17,33,4,18,16,64,0,18,50,4,129,2,18,64,0,89,50,4,129,3,18,64,0,93,36,67,51,0,17,37,18,51,0,16,34,18,16,51,0,18,36,18,16,51,0,1,33,5,14,16,51,0,32,50,3,18,16,51,0,21,50,3,18,16,51,1,17,33,4,18,16,51,1,16,34,18,16,51,1,18,36,18,16,51,1,1,33,5,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,67,51,0,16,35,18,51,1,16,34,18,16,67,51,0,16,35,18,51,1,16,34,18,16,51,2,16,129,1,18,16]},"central_escrow":{"address":"P7GEWDXXW5IONRW6XRIRVPJCT2XXEQGOBGG65VJPBUOYZEJCBZWTPHS3VQ","program":[4,129,1]},"customer_escrow":{"address":"3BW2V2NE7AIFGSARHF7ULZFWJPCOYOJTP3NL6ZQ3TWMSK673HTWTPPKEBA","program":[4,32,1,1,50,4,129,3,18,64,0,3,129,0,67,51,0,16,129,6,18,51,1,16,34,18,16,51,1,1,129,232,7,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,51,1,7,128,32,127,204,75,14,247,183,80,230,198,222,188,81,26,189,34,158,175,114,64,206,9,141,238,213,47,13,29,140,145,34,14,109,18,16,51,2,16,34,18,16]},"uuid":"f5c8614f-f969-4e65-8039-15048a5055dd"}"#;
        let project_json = serde_json::from_str::<ProjectJson>(json)?;

        let project = project_json.try_into().map_err(Error::msg)?;

        let id = project_dao.save_project(&project).await?;
        println!("id: {:?}", id);

        let loaded_project = project_dao.load_project(id.parse()?).await?;
        // println!("project: {:?}", loaded_project);

        assert_eq!(project, loaded_project);

        Ok(())
    }

    async fn create_test_project_dao() -> Result<Box<dyn ProjectDao>> {
        let client = create_db_client().await?;
        Ok(Box::new(ProjectDaoImpl {
            client: Arc::new(client),
        }))
    }
}
