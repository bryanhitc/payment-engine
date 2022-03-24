use anyhow::anyhow;
use log::info;

use payment_engine::engine::{PaymentEngine, SerialPaymentEngine};

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
    // let mut engine = StreamPaymentEngine::default();
    let mut engine = SerialPaymentEngine::default();

    for row in reader.deserialize() {
        let transaction = row?;
        engine.process(transaction)?;
    }

    let worker_results = engine.finalize();

    {
        let stdio = std::io::stdout().lock();
        let mut writer = csv::Writer::from_writer(stdio);

        for result in worker_results {
            let snapshot = result?;
            writer.serialize(&snapshot)?;
        }
    }

    Ok(())
}
