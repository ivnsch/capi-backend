use super::withdrawal_dao::WithdrawalDao;
use anyhow::Result;
use chrono::Utc;
use core_::api::model::{SavedWithdrawal, Withdrawal, WithdrawalInputs};

pub async fn save_withdrawal(
    dao: &dyn WithdrawalDao,
    withdrawal: &WithdrawalInputs,
) -> Result<SavedWithdrawal> {
    let withdrawal = Withdrawal {
        project_id: withdrawal.project_id.clone(),
        amount: withdrawal.amount,
        description: withdrawal.description.clone(),
        date: Utc::now(),
    };
    let saved_withdrawal = dao.save_withdrawal(&withdrawal).await?;
    Ok(saved_withdrawal)
}

pub async fn load_withdrawals(
    dao: &dyn WithdrawalDao,
    project_id: &str,
) -> Result<Vec<SavedWithdrawal>> {
    let withdrawals = dao.load_withdrawals(project_id.parse()?).await?;
    Ok(withdrawals)
}
