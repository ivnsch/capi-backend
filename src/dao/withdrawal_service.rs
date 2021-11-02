use super::withdrawal_dao::WithdrawalDao;
use anyhow::Result;
use chrono::Utc;
use core_::api::model::{SavedWithdrawalRequest, WithdrawalRequest, WithdrawalRequestInputs};

pub async fn save_withdrawal_request(
    dao: &dyn WithdrawalDao,
    request: &WithdrawalRequestInputs,
) -> Result<SavedWithdrawalRequest> {
    let request = WithdrawalRequest {
        project_id: request.project_id.clone(),
        slot_id: request.slot_id.clone(),
        amount: request.amount,
        description: request.description.clone(),
        date: Utc::now(),
        complete: false, // new request: hasn't been completed yet
    };
    let saved_request = dao.save_withdrawal_request(&request).await?;
    Ok(saved_request)
}

pub async fn complete_withdrawal_request(dao: &dyn WithdrawalDao, request_id: &str) -> Result<()> {
    dao.complete_withdrawal_request(&request_id.parse()?)
        .await?;
    Ok(())
}

pub async fn load_withdrawal_requests(
    dao: &dyn WithdrawalDao,
    project_id: &str,
) -> Result<Vec<SavedWithdrawalRequest>> {
    let requests = dao.load_withdrawal_requests(project_id.parse()?).await?;
    Ok(requests)
}
