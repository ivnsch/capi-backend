use std::{
    convert::{Infallible, TryInto},
    sync::Arc,
};

use anyhow::Result;
use core_::{
    api::{
        json_workaround::{ProjectForUsersJson, ProjectJson},
        model::ProjectForUsers,
    },
    flows::create_project::model::Project,
};
use dao::project_dao::ProjectDao;
use logger::init_logger;
use warp::Filter;

use crate::dao::{db::create_db_client, project_dao::ProjectDaoImpl, project_service};
use dotenv::dotenv;
use std::env;

mod dao;
mod logger;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();

    let db_client = Arc::new(create_db_client().await?);
    let project_dao: Arc<dyn ProjectDao> = Arc::new(ProjectDaoImpl {
        client: db_client.clone(),
    });
    project_dao.init().await?;

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

    // project "view" for UI. TODO rename
    let invest_project = warp::get()
        .and(warp::path!("invest" / String))
        .and(with_env(env.clone()))
        .and(with_project_dao(project_dao.clone()))
        .and_then(|id: String, env, dao: Arc<dyn ProjectDao>| async {
            handle_get_project_for_users(dao, env, id).await
        })
        .with(cors.clone())
        .with(warp::log("get invest_project log"));

    // project "view" for UI. TODO rename
    let invest_project_with_uuid = warp::get()
        .and(warp::path!("invest_with_uuid" / String))
        .and(with_env(env.clone()))
        .and(with_project_dao(project_dao.clone()))
        .and_then(|id: String, env, dao: Arc<dyn ProjectDao>| async {
            handle_get_project_for_users_with_uuid(dao, env, id).await
        })
        .with(cors.clone())
        .with(warp::log("get invest_project_with_uuid log"));

    let load_project = warp::get()
        .and(warp::path!("project" / String))
        .and(with_project_dao(project_dao.clone()))
        .and_then(|id: String, dao: Arc<dyn ProjectDao>| async {
            handle_get_project(dao, id).await
        })
        .with(cors.clone())
        .with(warp::log("get load_project log"));

    let load_project_with_uuid = warp::get()
        .and(warp::path!("project_with_uuid" / String))
        .and(with_project_dao(project_dao))
        .and_then(|id: String, dao: Arc<dyn ProjectDao>| async {
            handle_get_project_with_uuid(dao, id).await
        })
        .with(cors.clone())
        .with(warp::log("get load_project log"));

    warp::serve(
        save_project
            .or(invest_project)
            .or(invest_project_with_uuid)
            .or(load_project)
            .or(load_project_with_uuid),
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

async fn handle_get_project_for_users_with_uuid(
    project_dao: Arc<dyn ProjectDao>,
    env: Env,
    uuid: String,
) -> Result<impl warp::Reply, Infallible> {
    let res = project_service::load_project_for_users_with_uuid(&*project_dao, &env, &uuid).await;
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

async fn handle_get_project_with_uuid(
    project_dao: Arc<dyn ProjectDao>,
    uuid: String,
) -> Result<impl warp::Reply, Infallible> {
    let res = project_service::load_project_with_uuid(&*project_dao, &uuid).await;
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

fn frontend_host(env: &Env) -> &'static str {
    match env {
        Env::Local => "http://localhost:3000",
        Env::Test => "http://test.app.capi.money",
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
