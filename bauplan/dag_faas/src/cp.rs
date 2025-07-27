use crate::dag_proto::dag_proto::worker_client::WorkerClient;
use crate::dag_proto::dag_proto::TaskRequest;
use petgraph::algo::toposort;
use petgraph::graph::NodeIndex;
use crate::dag::build_sample_dag;
use crate::arrow_util::*;
use std::collections::HashMap;

// Control Plane (CP) - orchestrates the execution of tasks across multiple workers
pub async fn run_cp(worker_addrs: Vec<&str>) {
    let (dag, _) = build_sample_dag();
    let topo = toposort(&dag, None).expect("DAG must be acyclic");
    let mut node_results: HashMap<NodeIndex, Vec<u8>> = HashMap::new();

    for (i, node_idx) in topo.iter().enumerate() {
        let node = &dag[*node_idx];
        let worker_addr = worker_addrs[i % worker_addrs.len()];

        let parent_outputs: Vec<Vec<u8>> = dag
            .neighbors_directed(*node_idx, petgraph::Incoming)
            .map(|parent| node_results.get(&parent).cloned().unwrap_or_default())
            .collect();

        println!("CP: dispatching node {} to worker {}", node.id, worker_addr);

        let mut client = WorkerClient::connect(worker_addr.to_string()).await.unwrap();
        let req = tonic::Request::new(TaskRequest {
            task_id: node.id.clone(),
            code: node.code.clone(),
            input_batches: parent_outputs,
        });
        let resp = client.run_task(req).await.unwrap().into_inner();
        println!("CP: got result for node {}: {}", node.id, resp.log);

        node_results.insert(*node_idx, resp.output_batch);
    }

    let last_idx = *topo.last().unwrap();
    let final_batch = bytes_to_batch(&node_results[&last_idx]);
    println!("Final result:");
    for row in 0..final_batch.num_rows() {
        let country = final_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(row);
        let usd_sum = final_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .value(row);
        println!("country: {}, usd_sum: {}", country, usd_sum);
    }
}
