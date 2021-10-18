use std::sync::Arc;

use algonaut::transaction::account::ContractAccount;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use data_encoding::BASE64;
use make::flows::create_project::model::{CreateProjectSpecs, CreateSharesSpecs, Project};
use tokio_postgres::Client;

use super::db::{get_address, get_bytes, get_microalgos, get_u64};

#[async_trait]
pub trait ProjectDao: Sync + Send {
    async fn init(&self) -> Result<()>;

    async fn save_project(&self, project: &Project) -> Result<String>;
    async fn load_project(&self, id: i32) -> Result<Project>;
}
pub struct ProjectDaoImpl {
    pub client: Arc<Client>,
}

#[async_trait]
impl ProjectDao for ProjectDaoImpl {
    async fn init(&self) -> Result<()> {
        // TODO slots 1:n
        let _ = self
            .client
            .execute(
                "CREATE TABLE IF NOT EXISTS project(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            creator TEXT NOT NULL,
            asset_price TEXT NOT NULL,
            vote_threshold TEXT NOT NULL,
            token_name TEXT NOT NULL,
            share_count TEXT NOT NULL,
            share_id TEXT NOT NULL,
            app_id TEXT NOT NULL,
            slot1_id TEXT NOT NULL,
            slot2_id TEXT NOT NULL,
            slot3_id TEXT NOT NULL,
            invest_e TEXT NOT NULL,
            invest_b TEXT NOT NULL,
            staking_e TEXT NOT NULL,
            staking_b TEXT NOT NULL,
            central_e TEXT NOT NULL,
            central_b TEXT NOT NULL,
            customer_e TEXT NOT NULL,
            customer_b TEXT NOT NULL
        );",
                &[],
            )
            .await?;
        // note: execute returns "rows modified", for create table it's always 0
        Ok(())
    }

    async fn save_project(&self, project: &Project) -> Result<String> {
        // TODO slots 1:n
        assert!(project.withdrawal_slot_ids.len() == 3);

        let id_rows = self.client
            .query(
                "INSERT INTO project (name, creator, asset_price, vote_threshold, token_name, share_count, share_id, app_id, slot1_id, slot2_id, slot3_id, invest_e, invest_b, staking_e, staking_b, central_e, central_b, customer_e, customer_b) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING id;",
                &[
                    &project.specs.name,
                    &project.creator.to_string(),
                    &project.specs.asset_price.0.to_string(),
                    &project.specs.vote_threshold.to_string(),
                    &project.specs.shares.token_name.to_string(),
                    &project.specs.shares.count.to_string(),
                    &project.shares_asset_id.to_string(),
                    &project.central_app_id.to_string(),
                    &project.withdrawal_slot_ids[0].to_string(),
                    &project.withdrawal_slot_ids[1].to_string(),
                    &project.withdrawal_slot_ids[2].to_string(),
                    &project.invest_escrow.address.to_string(),
                    &BASE64.encode(&project.invest_escrow.program.0),
                    &project.staking_escrow.address.to_string(),
                    &BASE64.encode(&project.staking_escrow.program.0),
                    &project.central_escrow.address.to_string(),
                    &BASE64.encode(&project.central_escrow.program.0),
                    &project.customer_escrow.address.to_string(),
                    &BASE64.encode(&project.customer_escrow.program.0),
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
            "SELECT name, asset_price, vote_threshold, token_name, share_count, creator, share_id, app_id, slot1_id, slot2_id, slot3_id, invest_e, invest_b, staking_e, staking_b, central_e, central_b, customer_e, customer_b FROM project WHERE id=$1;", 
            &[&id]).await?;

        let project_row = match project_rows.as_slice() {
            [row] => row,
            _ => return Err(anyhow!("Project not found: {}", id)),
        };

        Ok(Project {
            specs: CreateProjectSpecs {
                name: project_row.get(0),
                asset_price: get_microalgos(project_row, 1)?,
                vote_threshold: get_u64(project_row, 2)?,
                shares: CreateSharesSpecs {
                    token_name: project_row.get(3),
                    count: get_u64(project_row, 4)?,
                },
            },
            creator: get_address(project_row, 5)?,
            shares_asset_id: get_u64(project_row, 6)?,
            central_app_id: get_u64(project_row, 7)?,
            withdrawal_slot_ids: vec![
                get_u64(project_row, 8)?,
                get_u64(project_row, 9)?,
                get_u64(project_row, 10)?,
            ],
            invest_escrow: ContractAccount {
                address: get_address(project_row, 11)?,
                program: get_bytes(project_row, 12)?,
            },
            staking_escrow: ContractAccount {
                address: get_address(project_row, 13)?,
                program: get_bytes(project_row, 14)?,
            },
            central_escrow: ContractAccount {
                address: get_address(project_row, 15)?,
                program: get_bytes(project_row, 16)?,
            },
            customer_escrow: ContractAccount {
                address: get_address(project_row, 17)?,
                program: get_bytes(project_row, 18)?,
            },
        })
    }
}

#[cfg(test)]
mod test {
    use std::{convert::TryInto, sync::Arc};

    use super::{ProjectDao, ProjectDaoImpl};
    use crate::dao::db::create_db_client;
    use anyhow::{Error, Result};
    use logger::init_logger;
    use make::api::json_workaround::ProjectJson;
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
        let json = r#"{"specs":{"name":"my1project","shares":{"token_name":"foo","count":100},"asset_price":1000000,"vote_threshold":70},"creator_address":"MKRBTLNZRS3UZZDS5OWPLP7YPHUDNKXFUFN5PNCJ3P2XRG74HNOGY6XOYQ","shares_asset_id":42,"central_app_id":50,"invest_escrow":{"address":"SV2LIUFR5AL2BZOMGW3SAYU5FT2T662NOXPVKXF3GKGTDYRZJMHENNZS2Y","program":[4,32,6,6,42,0,232,7,43,4,50,4,34,18,51,2,17,35,18,16,51,3,17,33,4,18,16,64,0,9,50,4,34,18,64,0,83,36,67,51,2,17,35,18,51,2,16,33,5,18,16,51,2,18,36,18,16,51,2,1,37,14,16,51,2,32,50,3,18,16,51,2,21,50,3,18,16,51,3,17,33,4,18,16,51,3,16,33,5,18,16,51,3,18,36,18,16,51,3,1,37,14,16,51,3,32,50,3,18,16,51,3,21,50,3,18,16,66,0,91,51,0,16,34,18,51,3,17,35,18,16,51,3,20,128,32,247,10,15,104,164,223,249,27,116,139,66,224,167,91,33,215,215,35,34,187,44,221,159,36,227,39,167,77,162,152,169,0,18,16,51,3,1,37,14,16,51,3,21,50,3,18,16,51,3,32,50,3,18,16,51,1,8,51,3,18,129,192,132,61,11,18,16,51,3,18,51,4,18,18,16]},"staking_escrow":{"address":"64FA62FE374RW5ELILQKOWZB27LSGIV3FTOZ6JHDE6TU3IUYVEAKZXC3DQ","program":[4,32,6,4,6,0,42,43,232,7,50,4,35,18,51,0,17,37,18,16,51,1,17,33,4,18,16,64,0,18,50,4,129,2,18,64,0,89,50,4,129,3,18,64,0,93,36,67,51,0,17,37,18,51,0,16,34,18,16,51,0,18,36,18,16,51,0,1,33,5,14,16,51,0,32,50,3,18,16,51,0,21,50,3,18,16,51,1,17,33,4,18,16,51,1,16,34,18,16,51,1,18,36,18,16,51,1,1,33,5,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,67,51,0,16,35,18,51,1,16,34,18,16,67,51,0,16,35,18,51,1,16,34,18,16,51,2,16,129,1,18,16]},"central_escrow":{"address":"P7GEWDXXW5IONRW6XRIRVPJCT2XXEQGOBGG65VJPBUOYZEJCBZWTPHS3VQ","program":[4,129,1]},"customer_escrow":{"address":"3BW2V2NE7AIFGSARHF7ULZFWJPCOYOJTP3NL6ZQ3TWMSK673HTWTPPKEBA","program":[4,32,1,1,50,4,129,3,18,64,0,3,129,0,67,51,0,16,129,6,18,51,1,16,34,18,16,51,1,1,129,232,7,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,51,1,7,128,32,127,204,75,14,247,183,80,230,198,222,188,81,26,189,34,158,175,114,64,206,9,141,238,213,47,13,29,140,145,34,14,109,18,16,51,2,16,34,18,16]}, "withdrawal_slot_ids":[1,2,3]}"#;
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
