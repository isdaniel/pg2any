//! Phase 1 dead-work demo: finalizing an uncompressed transaction file no
//! longer re-reads the file from disk to count its statements.
//!
//! After Phase 1, `UncompressedStorage::write_transaction_from_file(path, n)`
//! is O(1): the statement count is supplied by the producer, so the finalize
//! path never opens or scans the file. This demo contrasts that O(1) call with
//! the manual re-read + line-count loop the code used to perform.
//!
//! Run: cargo run -p pg2any --bin phase1_deadwork_demo

use pg2any_lib::storage::{TransactionStorage, UncompressedStorage};
use std::io::Write;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: usize = 50_000;

    // Write a large temp .sql file (one statement per line).
    let path = std::env::temp_dir().join("pg2any_phase1_deadwork_demo.sql");
    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path)?);
        for i in 0..n {
            writeln!(f, "INSERT INTO demo (id, val) VALUES ({i}, 'row-{i}');")?;
        }
        f.flush()?;
    }
    println!("Wrote {n} statements to {path:?}");

    // (a) NEW O(1) finalize: count is known, file is never re-read.
    let storage = UncompressedStorage::new();
    let start = Instant::now();
    storage.write_transaction_from_file(&path, n).await?;
    let new_elapsed = start.elapsed();

    // (b) OLD behavior: re-open the file and count lines to learn the count.
    let start = Instant::now();
    let contents = std::fs::read_to_string(&path)?;
    let counted = contents.lines().count();
    let old_elapsed = start.elapsed();
    assert_eq!(counted, n, "sanity: re-read count must match");

    let speedup = old_elapsed.as_secs_f64() / new_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("Phase 1 demo: finalize of {n} statements");
    println!("  NEW (O(1), no re-read): {new_elapsed:?}");
    println!("  OLD (re-read + count):  {old_elapsed:?}");
    println!("  speedup: {speedup:.1}x (dead disk work removed from the commit/finalize path)");

    let _ = std::fs::remove_file(&path);
    Ok(())
}
