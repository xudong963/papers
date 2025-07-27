use std::env;

mod arrow_util;
mod dag;
mod worker;
mod dag_proto;
mod dp;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} [dp|worker <addr>]", args[0]);
        return;
    }
    match args[1].as_str() {
        "worker" => {
            let addr = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:50051");
            println!("Starting worker at {}", addr);
            worker::serve_worker(addr).await;
        }
        "dp" => {
            let workers = vec!["http://127.0.0.1:50051", "http://127.0.0.1:50052"];
            dp::run_dp(workers).await;
        }
        _ => {
            println!("Unknown command");
        }
    }
}
