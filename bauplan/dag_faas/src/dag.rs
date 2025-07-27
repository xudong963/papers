use petgraph::graph::{DiGraph, NodeIndex};

#[derive(Debug, Clone)]
pub struct DagNode {
    pub id: String,
    pub code: String, // "source" | "filter_country" | "groupby_sum"
}

pub fn build_sample_dag() -> (DiGraph<DagNode, ()>, NodeIndex) {
    let mut dag = DiGraph::<DagNode, ()>::new();
    let idx_transactions = dag.add_node(DagNode { id: "transactions".to_string(), code: "source".to_string() });
    let idx_euro = dag.add_node(DagNode { id: "euro_selection".to_string(), code: "filter_country".to_string() });
    let idx_usd = dag.add_node(DagNode { id: "usd_by_country".to_string(), code: "groupby_sum".to_string() });
    dag.add_edge(idx_transactions, idx_euro, ());
    dag.add_edge(idx_euro, idx_usd, ());
    (dag, idx_transactions)
}
