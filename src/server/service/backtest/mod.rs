use std::collections;
use serde::Serialize;
use super::super::message::instruction::{
    MarketType,
    BacktestRequestInstruction,
};

#[derive(Serialize, Clone)]
pub enum BacktestTaskStatus {
    Queueing,
    Running,
    Completed,
    Failed,
    Canceled,
    NotFound,
}

#[derive(Serialize)]
pub struct BacktestTaskInfo {
    pub r#ref: String,
    pub status: BacktestTaskStatus
}

pub struct BacktestTask {
    reference: String,
    status: BacktestTaskStatus,
}

impl BacktestTask {
    pub fn new(ref_str: String, status: BacktestTaskStatus) -> Self {
        BacktestTask {
            reference: ref_str,
            status,
        }
    }

    pub fn get_info(&self) -> BacktestTaskInfo {
        BacktestTaskInfo {
            r#ref: self.reference.clone(),
            status: self.status.clone(),
        }
    }
}

pub struct BacktestManager {
    queue_limit: usize,
    task_queue: collections::LinkedList<BacktestTask>
}

impl BacktestManager {
    pub fn new(queue_limit: usize) -> Self {
        BacktestManager {
            queue_limit,
            task_queue: collections::LinkedList::new(),
        }
    }

    pub fn new_task(&self, instr: &BacktestRequestInstruction) -> Result<BacktestTaskInfo, String> {
        if self.task_queue.len() >= self.queue_limit {
            return Err(format!("Backtest queue is full (limit: {})", self.queue_limit));
        }
        let task = BacktestTask::new(String::new(), BacktestTaskStatus::Canceled);
        // self.task_queue.push_back(task.clone());
        Ok(task.get_info())
    }

    pub fn cancel_task(&self, _ref: &String) -> Result<BacktestTaskInfo, String> {
        Ok(BacktestTaskInfo {
            r#ref: _ref.clone(),
            status: BacktestTaskStatus::NotFound,
        })
    }

    pub fn query_task(&self, _ref: &String) -> Result<BacktestTaskInfo, String> {
        Ok(BacktestTaskInfo {
            r#ref: _ref.clone(),
            status: BacktestTaskStatus::NotFound,
        })
    }
}