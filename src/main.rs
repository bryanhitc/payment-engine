#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use anyhow::anyhow;
use log::info;

use payment_engine::engine::{Engine, PaymentEngine};

fn main() -> anyhow::Result<()> {
    // Since the executable name is always the first argument, we must skip to
    // the second one (which is the first "real" user-specified arg) to get the file name.
    let input_file_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("No input file path specified"))?;
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .flexible(true)
        .from_path(&input_file_path)?;

    info!("Reading input from {input_file_path}");

    // TODO (PERF + CORRECTNESS): Address StreamPaymentEngine's thread
    // issue (N threads where N = unique clients... need a threadpool)
    // and enable it by default.
    //
    // I included this anyway to show give you a good high-level idea of
    // how I think it may work. In practice, this would connect to a
    // distributed queue + enqueue => worker nodes pull.
    let mut engine = Engine::default();
    for row in reader.deserialize() {
        let transaction = row?;
        engine.process(transaction)?;
    }

    let worker_results = engine.finalize();
    {
        let stdout = std::io::stdout();
        let stdio = stdout.lock();
        let mut writer = csv::Writer::from_writer(stdio);
        for result in worker_results {
            let snapshot = result?;
            writer.serialize(&snapshot)?;
        }
    }

    Ok(())
}
