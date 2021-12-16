use super::db::get_u64;
use algonaut::core::MicroAlgos;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use core_::api::model::{SavedWithdrawal, Withdrawal};
use std::sync::Arc;
use tokio_postgres::{Client, Row};

#[async_trait]
pub trait WithdrawalDao: Sync + Send {
    async fn init(&self) -> Result<()>;

    async fn save_withdrawal(&self, withdrawal: &Withdrawal) -> Result<SavedWithdrawal>;

    async fn load_withdrawals(&self, project_id: i32) -> Result<Vec<SavedWithdrawal>>;
}
pub struct WithdrawalDaoImpl {
    pub client: Arc<Client>,
}

#[async_trait]
impl WithdrawalDao for WithdrawalDaoImpl {
    async fn init(&self) -> Result<()> {
        let _ = self
            .client
            .execute(
                "CREATE TABLE IF NOT EXISTS withdrawal(
                id SERIAL PRIMARY KEY,
                project_id integer NOT NULL,
                amount TEXT NOT NULL,
                description TEXT NOT NULL,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                CONSTRAINT fk_project
                    FOREIGN KEY(project_id) 
                    REFERENCES project(id)
                );",
                &[],
            )
            .await?;
        // note: execute returns "rows modified", for create table it's always 0
        Ok(())
    }

    async fn save_withdrawal(&self, withdrawal: &Withdrawal) -> Result<SavedWithdrawal> {
        let project_id: i32 = withdrawal.project_id.parse()?;
        let id_rows = self.client
            .query(
                "INSERT INTO withdrawal (project_id, amount, description, date) VALUES ($1, $2, $3, $4) RETURNING id;",
                &[
                    &project_id,
                    &withdrawal.amount.to_string(),
                    &withdrawal.description.to_string(),
                    &withdrawal.date,
                ],
            )
            .await?;

        log::debug!("Saved withdrawal: {:?}", withdrawal);

        let id_row = match id_rows.as_slice() {
            [row] => row,
            _ => return Err(anyhow!("Unexpected row count: {}", id_rows.len())),
        };
        let id: i32 = id_row.get(0);
        let id_str = id.to_string();

        log::debug!("Saved project, row id: {}", id_str);

        Ok(SavedWithdrawal {
            id: id_str,
            project_id: withdrawal.project_id.clone(),
            amount: withdrawal.amount,
            description: withdrawal.description.clone(),
            date: withdrawal.date,
        })
    }

    async fn load_withdrawals(&self, project_id: i32) -> Result<Vec<SavedWithdrawal>> {
        let project_rows = self.client.query(
            "SELECT id, project_id, amount, description, date FROM withdrawal WHERE project_id=$1 ORDER BY date DESC;",
            &[&project_id]).await?;

        fn to_obj(r: Row) -> Result<SavedWithdrawal> {
            Ok(SavedWithdrawal {
                id: r.get::<_, i32>(0).to_string(),
                project_id: r.get::<_, i32>(1).to_string(),
                amount: MicroAlgos(get_u64(&r, 2)?),
                description: r.get(3),
                date: r.get(4),
            })
        }
        project_rows.into_iter().map(to_obj).collect()
    }
}

#[cfg(test)]
mod test {
    use std::{convert::TryInto, sync::Arc};

    use crate::{
        dao::{
            db::create_db_client,
            project_dao::{ProjectDao, ProjectDaoImpl},
        },
        logger::init_logger,
    };
    use algonaut::core::MicroAlgos;
    use anyhow::{Error, Result};
    use chrono::Utc;
    use core_::api::{json_workaround::ProjectJson, model::Withdrawal};
    use tokio::test;

    use super::{WithdrawalDao, WithdrawalDaoImpl};

    #[test]
    #[ignore] // ignored until we've a test db and reset on each test
    async fn test_insert_and_load_a_withdrawal() -> Result<()> {
        init_logger();

        let client = Arc::new(create_db_client().await?);

        let project_dao = Box::new(ProjectDaoImpl {
            client: client.clone(),
        });
        project_dao.init().await?;
        let withdrawal_dao = Box::new(WithdrawalDaoImpl { client });
        withdrawal_dao.init().await?;

        // precs

        let project_id = insert_a_project(project_dao.as_ref()).await?;

        // test

        // insert a withdrawal
        let withdrawal = Withdrawal {
            project_id: project_id.clone(),
            amount: MicroAlgos(100_000),
            description: "Rent".to_owned(),
            date: Utc::now(),
        };
        let saved_withdrawal = withdrawal_dao.save_withdrawal(&withdrawal).await?;
        println!("saved_withdrawal: {:?}", saved_withdrawal);

        // load and check that it's equal to the withdrawal we inserted
        let withdrawals = withdrawal_dao.load_withdrawals(project_id.parse()?).await?;
        assert_eq!(1, withdrawals.len());
        let loaded_withdrawal = withdrawals[0].clone();
        assert_eq!(withdrawal.amount, loaded_withdrawal.amount);
        assert_eq!(withdrawal.description, loaded_withdrawal.description);
        assert_eq!(withdrawal.date, loaded_withdrawal.date);
        assert_eq!(withdrawal.project_id, loaded_withdrawal.project_id);

        Ok(())
    }

    async fn insert_a_project(project_dao: &dyn ProjectDao) -> Result<String> {
        let json = r#"{"specs":{"name":"my1project","shares":{"token_name":"foo","count":100},"investors_share":40,"asset_price":1000000},"creator_address":"MKRBTLNZRS3UZZDS5OWPLP7YPHUDNKXFUFN5PNCJ3P2XRG74HNOGY6XOYQ","shares_asset_id":42,"central_app_id":50,"invest_escrow":{"address":"SV2LIUFR5AL2BZOMGW3SAYU5FT2T662NOXPVKXF3GKGTDYRZJMHENNZS2Y","program":[4,32,6,6,42,0,232,7,43,4,50,4,34,18,51,2,17,35,18,16,51,3,17,33,4,18,16,64,0,9,50,4,34,18,64,0,83,36,67,51,2,17,35,18,51,2,16,33,5,18,16,51,2,18,36,18,16,51,2,1,37,14,16,51,2,32,50,3,18,16,51,2,21,50,3,18,16,51,3,17,33,4,18,16,51,3,16,33,5,18,16,51,3,18,36,18,16,51,3,1,37,14,16,51,3,32,50,3,18,16,51,3,21,50,3,18,16,66,0,91,51,0,16,34,18,51,3,17,35,18,16,51,3,20,128,32,247,10,15,104,164,223,249,27,116,139,66,224,167,91,33,215,215,35,34,187,44,221,159,36,227,39,167,77,162,152,169,0,18,16,51,3,1,37,14,16,51,3,21,50,3,18,16,51,3,32,50,3,18,16,51,1,8,51,3,18,129,192,132,61,11,18,16,51,3,18,51,4,18,18,16]},"staking_escrow":{"address":"64FA62FE374RW5ELILQKOWZB27LSGIV3FTOZ6JHDE6TU3IUYVEAKZXC3DQ","program":[4,32,6,4,6,0,42,43,232,7,50,4,35,18,51,0,17,37,18,16,51,1,17,33,4,18,16,64,0,18,50,4,129,2,18,64,0,89,50,4,129,3,18,64,0,93,36,67,51,0,17,37,18,51,0,16,34,18,16,51,0,18,36,18,16,51,0,1,33,5,14,16,51,0,32,50,3,18,16,51,0,21,50,3,18,16,51,1,17,33,4,18,16,51,1,16,34,18,16,51,1,18,36,18,16,51,1,1,33,5,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,67,51,0,16,35,18,51,1,16,34,18,16,67,51,0,16,35,18,51,1,16,34,18,16,51,2,16,129,1,18,16]},"central_escrow":{"address":"P7GEWDXXW5IONRW6XRIRVPJCT2XXEQGOBGG65VJPBUOYZEJCBZWTPHS3VQ","program":[4,129,1]},"customer_escrow":{"address":"3BW2V2NE7AIFGSARHF7ULZFWJPCOYOJTP3NL6ZQ3TWMSK673HTWTPPKEBA","program":[4,32,1,1,50,4,129,3,18,64,0,3,129,0,67,51,0,16,129,6,18,51,1,16,34,18,16,51,1,1,129,232,7,14,16,51,1,32,50,3,18,16,51,1,21,50,3,18,16,51,1,7,128,32,127,204,75,14,247,183,80,230,198,222,188,81,26,189,34,158,175,114,64,206,9,141,238,213,47,13,29,140,145,34,14,109,18,16,51,2,16,34,18,16]}}"#;
        let project_json = serde_json::from_str::<ProjectJson>(json)?;
        let project = project_json.try_into().map_err(Error::msg)?;
        let id = project_dao.save_project(&project).await?;
        Ok(id)
    }
}
