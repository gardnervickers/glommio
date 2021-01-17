//! Benchmark the noop performance of Glommio. This is a stress test for the executor/reactor
//! infrastructure.

use glommio::{LocalExecutorBuilder, Task};
use lazy_static::lazy_static;
use std::cell::UnsafeCell;
use std::fmt;
use std::time::{Duration, Instant};

lazy_static! {
    static ref HISTOGRAM: UnsafeHistogram = UnsafeHistogram {
        hist: UnsafeCell::new(Some(
            hdrhistogram::Histogram::new_with_bounds(1, 100 * 1000, 3).unwrap()
        ))
    };
}

struct UnsafeHistogram {
    hist: UnsafeCell<Option<hdrhistogram::Histogram<u64>>>,
}

impl UnsafeHistogram {
    fn record(data: u64) {
        let histo = unsafe { &mut *HISTOGRAM.hist.get() };
        let histo = histo.as_mut();
        if let Some(hist) = histo {
            hist.record(data).unwrap()
        }
    }

    fn take() -> hdrhistogram::Histogram<u64> {
        let histo = unsafe { &mut *HISTOGRAM.hist.get() };
        let old =
            histo.replace(hdrhistogram::Histogram::new_with_bounds(1, 100 * 1000, 3).unwrap());
        old.unwrap()
    }
}

unsafe impl Send for UnsafeHistogram {}
unsafe impl Sync for UnsafeHistogram {}

struct Bench {
    num_tasks: u64,
    num_events: u64,
}

const BENCH_RUNS: &'static [Bench] = &[
    Bench {
        num_tasks: 100,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 1_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 10_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 100_000,
        num_events: 10_000_000,
    },
    Bench {
        num_tasks: 1_000_000,
        num_events: 10_000_000,
    },
];

struct Measurement {
    total_events: u64,
    total_tasks: u64,
    duration: Duration,
    histogram: hdrhistogram::Histogram<u64>,
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r"completed submission of {} noop events across {} tasks, {} per task
            duration: {:?}",
            self.total_events,
            self.total_tasks,
            self.total_events / self.total_tasks,
            self.duration
        )
    }
}

fn main() {
    let mut measurements = vec![];
    let num_bench_runs = 1;
    for bench in &BENCH_RUNS[0..] {
        for _ in 0..num_bench_runs {
            let ex = LocalExecutorBuilder::new().pin_to_cpu(0).make().unwrap();
            let measurement = ex.run(run_bench_tasks(bench.num_tasks, bench.num_events));

            println!("{}", measurement);
            println!(
                "50'th noop percentile: {}",
                measurement.histogram.value_at_quantile(0.50)
            );
            println!(
                "99'th noop percentile: {}",
                measurement.histogram.value_at_quantile(0.99)
            );
            println!(
                "99.9'th noop percentile: {}",
                measurement.histogram.value_at_quantile(0.999)
            );

            measurements.push(measurement);
        }

        let sum = measurements
            .iter()
            .fold(Duration::from_secs(0), |acc, v| acc + v.duration);
        let average = sum / num_bench_runs;
        println!("average bench duration: {:?}\n", average);
        measurements.clear();
    }
}

fn new_histogram() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::<u64>::new_with_bounds(1, 1000 * 30, 3).unwrap()
}

async fn run_bench_single_task(num_events: u32) {
    let start_time = Instant::now();
    let submitter = glommio::nop::NopSubmitter::new();
    for _ in 0..num_events {
        submitter.run_nop().await.unwrap();
    }
    let end_time = Instant::now();
    let duration = end_time - start_time;
    println!("completed {} events in {:?}", num_events, duration);
}

async fn run_bench_tasks(num_tasks: u64, num_events: u64) -> Measurement {
    let num_ops_per_task = num_events / num_tasks;
    let start_time = Instant::now();
    let mut handles = vec![];
    for _ in 0..num_tasks {
        let handle = Task::local(async move {
            let submitter = glommio::nop::NopSubmitter::new();

            for _ in 0..num_ops_per_task {
                //let start_time = Instant::now();
                submitter.run_nop().await.unwrap();
                //let duration = Instant::now() - start_time;
                //UnsafeHistogram::record(duration.as_millis() as _);
            }
        });
        handles.push(handle)
    }
    for handle in handles {
        handle.await;
    }
    let end_time = Instant::now();
    Measurement {
        total_events: num_events,
        total_tasks: num_tasks,
        duration: end_time - start_time,
        histogram: UnsafeHistogram::take(),
    }
}
