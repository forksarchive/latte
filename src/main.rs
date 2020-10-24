use std::cmp::max;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cassandra_cpp::Session;
use clap::Clap;
use futures::{SinkExt, Stream, StreamExt, Future, TryFutureExt, executor};
use futures::executor::LocalPool;
use futures::task::{LocalSpawnExt, SpawnExt};

use config::RunCommand;

use crate::config::{AppConfig, Command, ShowCommand};
use crate::progress::FastProgressBar;
use crate::report::{Report, RunConfigCmp};
use crate::session::*;
use crate::stats::{BenchmarkCmp, BenchmarkStats, RequestStats, Recorder};
use crate::workload::{Workload, WorkloadStats};
use crate::workload::null::Null;
use crate::workload::read::Read;
use crate::workload::write::Write;
use futures::channel::mpsc::{Sender, Receiver};

mod config;
mod progress;
mod report;
mod session;
mod stats;
mod workload;

/// Reports an error and aborts the program if workload creation fails.
/// Returns unwrapped workload.
fn unwrap_workload<W: Workload>(w: workload::Result<W>) -> W {
    match w {
        Ok(w) => w,
        Err(e) => {
            eprintln!("error: Failed to initialize workload: {}", e);
            exit(1);
        }
    }
}

async fn workload(conf: &RunCommand, session: Session) -> Arc<dyn Workload> {
    let session = Box::new(session);
    let wc = conf.workload_config();
    match conf.workload {
        config::Workload::Read => Arc::new(unwrap_workload(Read::new(session, &wc).await)),
        config::Workload::Write => Arc::new(unwrap_workload(Write::new(session, &wc).await)),
        config::Workload::Null => Arc::new(Null {}),
    }
}

/// Rounds the duration down to the highest number of whole periods
fn round(duration: Duration, period: Duration) -> Duration {
    let mut duration = duration.as_micros();
    duration /= period.as_micros();
    duration *= period.as_micros();
    Duration::from_micros(duration as u64)
}


/// Runs a series of requests on a separate thread and
/// produces a stream of QueryStats
fn req_stream<F, C, R, E>(
    parallelism: usize,
    rate: Option<f64>,
    context: Arc<C>,
    action: F,
) -> impl Stream<Item = RequestStats>
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + Copy + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<WorkloadStats, E>> + Send,
    E: Send + 'static
{
    let (mut tx, rx) = futures::channel::mpsc::channel(65536);
    let rate = rate.map(|r| (r * 1000.0) as usize).unwrap_or(usize::MAX);

    let mut stream = futures::stream::repeat(())
        .enumerate()
        .map(move |(i, _)| {
            let context = context.clone();
            async move {
                let start = Instant::now();
                let result = action(context, i as u64).await;
                let end = Instant::now();
                RequestStats::from_result(result, end - start)
            }
        })
        .buffer_unordered(parallelism)
        .map(Ok)
        .forward(tx);

    futures::executor::ThreadPool::builder()
        .pool_size(1)
        .create()
        .unwrap()
        .spawn_ok(async move {
            match stream.await {
                Ok(()) => println!("Done"),
                Err(e) => println!("Error ***: {}", e)
            }
        });

    rx
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of iterations
///   - `parallelism`: maximum number of concurrent executions of `action`
///   - `rate`: optional rate limit given as number of calls to `action` per second
///   - `context`: a shared object to be passed to all invocations of `action`,
///      used to share e.g. a Cassandra session or Workload
///   - `action`: an async function to call; this function may fail and return an `Err`
async fn par_execute<F, C, R, RE>(
    name: &str,
    count: u64,
    parallelism: usize,
    rate: Option<f64>,
    sampling_period: Duration,
    context: Arc<C>,
    action: F,
) -> BenchmarkStats
where
    F: Fn(Arc<C>, u64) -> R + Send + Sync + Copy + 'static,
    C: ?Sized + Send + Sync + 'static,
    R: Future<Output = Result<WorkloadStats, RE>> + Send,
    RE: Send + 'static,
{
    let progress = FastProgressBar::new_progress_bar(name, count);
    let mut stats = Recorder::start(rate, parallelism);

    let mut stream = req_stream(parallelism, rate, context, action).take(count as usize);
    while let Some(r) = stream.next().await {
        stats.record(r);
        progress.tick();

        // let now = Instant::now();
        // if now - stats.last_sample_time() > sampling_period {
        //     let start_time = stats.start_time;
        //     let elapsed_rounded = round(now - start_time, sampling_period);
        //     let sample_time = start_time + elapsed_rounded;
        //     let log_line = stats.sample(sample_time).to_string();
        //     progress.println(log_line);
        // }
    }
    stats.finish()
}

fn load_report_or_abort(path: &PathBuf) -> Report {
    match Report::load(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to load report from {}: {}", path.display(), e);
            exit(1)
        }
    }
}

async fn run(conf: RunCommand) {
    let conf = conf.set_timestamp_if_empty();
    let compare = conf.compare.as_ref().map(|p| load_report_or_abort(p));

    let mut cluster = cluster(&conf);
    let session = connect_or_abort(&mut cluster).await;
    setup_keyspace_or_abort(&conf, &session).await;
    let session = connect_keyspace_or_abort(&mut cluster, conf.keyspace.as_str()).await;
    let workload = workload(&conf, session).await;

    println!(
        "{}",
        RunConfigCmp {
            v1: &conf,
            v2: compare.as_ref().map(|c| &c.conf)
        }
    );

    par_execute(
        "Populating...",
        workload.populate_count(),
        conf.parallelism,
        None, // make it as fast as possible
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.populate(i),
    )
    .await;

    par_execute(
        "Warming up...",
        conf.warmup_count,
        conf.parallelism,
        None,
        Duration::from_secs(u64::MAX),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    report::print_log_header();
    let stats = par_execute(
        "Running...",
        conf.count,
        conf.parallelism,
        conf.rate,
        Duration::from_secs_f64(conf.sampling_period),
        workload.clone(),
        |w, i| w.run(i),
    )
    .await;

    let stats_cmp = BenchmarkCmp {
        v1: &stats,
        v2: compare.as_ref().map(|c| &c.result),
    };
    println!();
    println!("{}", &stats_cmp);

    let path = conf
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(".latte-report.json"));

    let report = Report::new(conf, stats);
    match report.save(&path) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Failed to save report to {}: {}", path.display(), e);
            exit(1)
        }
    }
}

async fn show(conf: ShowCommand) {
    let report1 = load_report_or_abort(&PathBuf::from(conf.report1));
    let report2 = conf
        .report2
        .map(|p| load_report_or_abort(&PathBuf::from(p)));

    let config_cmp = RunConfigCmp {
        v1: &report1.conf,
        v2: report2.as_ref().map(|r| &r.conf),
    };
    println!("{}", config_cmp);

    let results_cmp = BenchmarkCmp {
        v1: &report1.result,
        v2: report2.as_ref().map(|r| &r.result),
    };
    println!("{}", results_cmp);
}

async fn async_main() {
    let command = AppConfig::parse().command;
    match command {
        Command::Run(config) => run(config).await,
        Command::Show(config) => show(config).await,
    }
}

fn main() {
    console::set_colors_enabled(true);
    executor::block_on(async_main());
}
