use anyhow::Result;
use make::{api::model::ProjectForUsers, flows::create_project::model::Project};

use crate::{frontend_host, Env};

use super::project_dao::ProjectDao;

pub async fn save_project(
    dao: &dyn ProjectDao,
    env: &Env,
    project: &Project,
) -> Result<ProjectForUsers> {
    let project_id = dao.save_project(project).await?;
    Ok(to_project_for_users(env, &project_id, project))
}

pub async fn load_project_for_users(
    dao: &dyn ProjectDao,
    env: &Env,
    id: &str,
) -> Result<ProjectForUsers> {
    let project = dao.load_project(id.parse()?).await?;
    Ok(to_project_for_users(env, id, &project))
}

pub async fn load_project(dao: &dyn ProjectDao, id: &str) -> Result<Project> {
    dao.load_project(id.parse()?).await
}

fn to_project_for_users(env: &Env, project_id: &str, project: &Project) -> ProjectForUsers {
    ProjectForUsers {
        id: project_id.to_owned(),
        name: project.specs.name.clone(),
        asset_price: project.specs.asset_price,
        investors_share: project.specs.investors_share,
        vote_threshold: project.specs.vote_threshold, // percent
        shares_asset_id: project.shares_asset_id,
        central_app_id: project.central_app_id,
        slot_ids: project.withdrawal_slot_ids.clone(),
        invest_escrow_address: project.invest_escrow.address,
        staking_escrow_address: project.staking_escrow.address,
        central_escrow_address: project.central_escrow.address,
        customer_escrow_address: project.customer_escrow.address,
        invest_link: format!("{}/invest/{}", frontend_host(env), project_id),
        my_investment_link: format!("{}/investment/{}", frontend_host(env), project_id),
        project_link: format!("{}/project/{}", frontend_host(env), project_id),
        creator: project.creator,
    }
}
