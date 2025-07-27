use tonic::{Request, Response, Status};
use crate::dag_proto::dag_proto::worker_server::{Worker, WorkerServer};
use crate::dag_proto::dag_proto::{TaskRequest, TaskResult};
use crate::arrow_util::*;

pub struct MyWorker {}

#[tonic::async_trait]
impl Worker for MyWorker {
    async fn run_task(&self, request: Request<TaskRequest>) -> Result<Response<TaskResult>, Status> {
        let req = request.into_inner();
        println!("Worker: received task {} code {}", req.task_id, req.code);

        let input_batches: Vec<_> = req.input_batches.iter().map(|b| bytes_to_batch(b)).collect();

        let output_batch = match req.code.as_str() {
            "source" => make_sample_batch(),
            "filter_country" => filter_country(&input_batches[0], "IT"),
            "groupby_sum" => groupby_sum(&input_batches[0]),
            _ => panic!("Unknown code"),
        };

        let output_bytes = batch_to_bytes(&output_batch);

        Ok(Response::new(TaskResult {
            task_id: req.task_id,
            log: format!("Worker finished {}", req.code),
            output_batch: output_bytes,
        }))
    }
}

pub async fn serve_worker(addr: &str) {
    let worker = MyWorker {};
    tonic::transport::Server::builder()
        .add_service(WorkerServer::new(worker))
        .serve(addr.parse().unwrap())
        .await
        .unwrap();
}
