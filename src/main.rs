use std::{
    convert::{Infallible, TryInto},
    sync::Arc,
};

use anyhow::Result;
use dao::{project_dao::ProjectDao, withdrawal_dao::WithdrawalDao};
use logger::init_logger;
use make::{
    api::{
        json_workaround::{ProjectForUsersJson, ProjectJson},
        model::{ProjectForUsers, WithdrawalRequestInputs},
    },
    flows::create_project::model::Project,
};
use warp::Filter;

use crate::dao::{
    db::create_db_client, project_dao::ProjectDaoImpl, project_service,
    withdrawal_dao::WithdrawalDaoImpl, withdrawal_service,
};
use dotenv::dotenv;
use std::env;

mod dao;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();

    let db_client = Arc::new(create_db_client().await?);
    let project_dao: Arc<dyn ProjectDao> = Arc::new(ProjectDaoImpl {
        client: db_client.clone(),
    });
    project_dao.init().await?;
    let withdrawal_dao: Arc<dyn WithdrawalDao> = Arc::new(WithdrawalDaoImpl { client: db_client });
    withdrawal_dao.init().await?;

    let env = environment();

    let cors = warp::cors()
        .allow_origin(frontend_host(&env))
        .allow_headers(vec![
            "User-Agent",
            "Sec-Fetch-Mode",
            "Referer",
            "Origin",
            "Content-Type",
            "Accept",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
        ])
        .allow_methods(vec!["GET", "POST"]);

    // TODO path project/save
    let save_project = warp::post()
        .and(warp::path!("save"))
        .and(warp::body::json())
        .and(with_env(env.clone()))
        .and(with_project_dao(project_dao.clone()))
        .and_then(|p: ProjectJson, env, dao: Arc<dyn ProjectDao>| async {
            handle_save_project(dao, env, p).await
        })
        .with(cors.clone())
        .with(warp::log("post save_project log"));

    // project "view" for investors. TODO rename
    let invest_project = warp::get()
        .and(warp::path!("invest" / String))
        .and(with_env(env.clone()))
        .and(with_project_dao(project_dao.clone()))
        .and_then(|id: String, env, dao: Arc<dyn ProjectDao>| async {
            handle_get_project_for_users(dao, env, id).await
        })
        .with(cors.clone())
        .with(warp::log("get invest_project log"));

    let load_project = warp::get()
        .and(warp::path!("project" / String))
        .and(with_project_dao(project_dao))
        .and_then(|id: String, dao: Arc<dyn ProjectDao>| async {
            handle_get_project(dao, id).await
        })
        .with(cors.clone())
        .with(warp::log("get load_project log"));

    let save_withdrawal_request = warp::post()
        .and(warp::path!("withdraw"))
        .and(warp::body::json())
        .and(with_withdrawal_dao(withdrawal_dao.clone()))
        .and_then(
            |request: WithdrawalRequestInputs, dao: Arc<dyn WithdrawalDao>| async {
                handle_save_withdrawal_request(dao, request).await
            },
        )
        .with(cors.clone())
        .with(warp::log("post save_withdrawal_request log"));

    let load_withdrawal_requests = warp::get()
        .and(warp::path!("withdrawals" / String))
        .and(with_withdrawal_dao(withdrawal_dao.clone()))
        .and_then(|id: String, dao: Arc<dyn WithdrawalDao>| async {
            handle_get_withdrawal_requests(dao, id).await
        })
        .with(cors.clone())
        .with(warp::log("get load_withdrawal_requests log"));

    // hack
    let complete_withdrawal_request = warp::post()
        .and(warp::path!("complete_withdrawal" / String))
        .and(with_withdrawal_dao(withdrawal_dao))
        .and_then(|id: String, dao: Arc<dyn WithdrawalDao>| async {
            handle_complete_withdrawal_request(dao, id).await
        })
        .with(cors.clone())
        .with(warp::log("get complete_withdrawal_request log"));

    warp::serve(
        save_project
            .or(invest_project)
            .or(load_project)
            .or(save_withdrawal_request)
            .or(load_withdrawal_requests)
            .or(complete_withdrawal_request),
    )
    // .run(([127, 0, 0, 1], 3030))
    .run(([0, 0, 0, 0], 3030))
    .await;

    Ok(())
}

fn with_env(env: Env) -> impl Filter<Extract = (Env,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || env.clone())
}

fn with_project_dao(
    dao: Arc<dyn ProjectDao>,
) -> impl Filter<Extract = (Arc<dyn ProjectDao>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || dao.clone())
}

fn with_withdrawal_dao(
    dao: Arc<dyn WithdrawalDao>,
) -> impl Filter<Extract = (Arc<dyn WithdrawalDao>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || dao.clone())
}

async fn handle_save_project(
    project_dao: Arc<dyn ProjectDao>,
    env: Env,
    project: ProjectJson,
) -> Result<impl warp::Reply, Infallible> {
    let project: Project = project.try_into().unwrap();
    log::debug!("got project: {:?}", project);

    let res = project_service::save_project(&*project_dao, &env, &project).await;
    log::debug!("handle_save_project res: {:?}", res);
    project_for_users_json(res)
}

async fn handle_get_project_for_users(
    project_dao: Arc<dyn ProjectDao>,
    env: Env,
    id: String,
) -> Result<impl warp::Reply, Infallible> {
    let res = project_service::load_project_for_users(&*project_dao, &env, &id).await;
    log::debug!("handle_get_project_for_users res: {:?}", res);
    project_for_users_json(res)
}

async fn handle_get_project(
    project_dao: Arc<dyn ProjectDao>,
    id: String,
) -> Result<impl warp::Reply, Infallible> {
    let res = project_service::load_project(&*project_dao, &id).await;
    log::debug!("handle_get_project res: {:?}", res);
    project_json(res)
}

fn project_for_users_json(res: Result<ProjectForUsers>) -> Result<impl warp::Reply, Infallible> {
    let json_res = res
        .map(ProjectForUsersJson::from)
        .map_err(|e| e.to_string());
    Ok(warp::reply::json(&json_res))
}

fn project_json(res: Result<Project>) -> Result<impl warp::Reply, Infallible> {
    let json_res = res.map(ProjectJson::from).map_err(|e| e.to_string());
    Ok(warp::reply::json(&json_res))
}

async fn handle_save_withdrawal_request(
    withdrawal_dao: Arc<dyn WithdrawalDao>,
    request: WithdrawalRequestInputs,
) -> Result<impl warp::Reply, Infallible> {
    log::debug!("json: {}", serde_json::to_string(&request).unwrap());
    let res = withdrawal_service::save_withdrawal_request(&*withdrawal_dao, &request)
        .await
        .map_err(|e| e.to_string());
    log::debug!("handle_save_withdrawal_request res: {:?}", res);
    Ok(warp::reply::json(&res))
    // Ok(warp::reply()) // empty reply
}

async fn handle_complete_withdrawal_request(
    withdrawal_dao: Arc<dyn WithdrawalDao>,
    request_id: String,
) -> Result<impl warp::Reply, Infallible> {
    log::debug!(
        "request_id: {}",
        serde_json::to_string(&request_id).unwrap()
    );
    let res = withdrawal_service::complete_withdrawal_request(&*withdrawal_dao, &request_id)
        .await
        .map_err(|e| e.to_string());
    log::debug!("handle_complete_withdrawal_request res: {:?}", res);
    Ok(warp::reply::json(&res))
}

async fn handle_get_withdrawal_requests(
    withdrawal_dao: Arc<dyn WithdrawalDao>,
    project_id: String,
) -> Result<impl warp::Reply, Infallible> {
    let res = withdrawal_service::load_withdrawal_requests(&*withdrawal_dao, &project_id)
        .await
        .map_err(|e| e.to_string());
    log::debug!("handle_get_withdrawal_requests res: {:?}", res);
    Ok(warp::reply::json(&res))
}

fn frontend_host(env: &Env) -> &'static str {
    match env {
        Env::Local => "http://localhost:3000",
        Env::Test => "http://test.app.nibble.money",
    }
}

#[derive(Debug, Clone)]
pub enum Env {
    Local,
    Test,
}

fn environment() -> Env {
    dotenv().ok();
    let env = env::var("TEST_ENV").unwrap();
    println!("Env value: {}", env);
    let env = if env == "1" { Env::Test } else { Env::Local };
    log::info!("Environment: {:?}", env);
    env
}
