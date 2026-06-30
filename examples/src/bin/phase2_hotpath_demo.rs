//! Phase 2 hot-path demo: counting statements in a transaction file no longer
//! allocates a `String` per statement.
//!
//! After Phase 2, `SqlStreamParser::count_line`/`finish_count` walk the same
//! quote/bracket/escape state machine as `parse_line` but only tally completed
//! statements -- they never buffer or allocate a `String`. This demo contrasts
//! that allocation-free count with the OLD approach (`parse_line` into a
//! `Vec<String>` + `finish_statement`), which materializes every statement.
//!
//! Best-of-N timing is used per path to drown out allocator/cache warmup noise.
//!
//! Run: cargo run --release -p pg2any --bin phase2_hotpath_demo

use pg2any_lib::storage::sql_parser::SqlStreamParser;
use std::hint::black_box;
use std::time::{Duration, Instant};

const N: usize = 50_000;
const ROUNDS: usize = 20;

/// NEW path: allocation-free count via `count_line` + `finish_count`.
fn count_new(lines: &[String]) -> usize {
    let mut parser = SqlStreamParser::new();
    let mut count = 0usize;
    for line in lines {
        count += parser.count_line(line);
    }
    if parser.finish_count() {
        count += 1;
    }
    count
}

/// OLD path: to learn the count you had to materialize every statement as an
/// owned `String` in a growing `Vec`, then read its length.
fn count_old(lines: &[String]) -> usize {
    let mut parser = SqlStreamParser::new();
    let mut out: Vec<String> = Vec::new();
    for line in lines {
        parser.parse_line(line, &mut out).expect("parse_line");
    }
    if let Some(last) = parser.finish_statement() {
        out.push(last);
    }
    out.len()
}

fn best_of(rounds: usize, mut f: impl FnMut() -> usize) -> (Duration, usize) {
    let mut best = Duration::MAX;
    let mut count = 0;
    for _ in 0..rounds {
        let start = Instant::now();
        count = black_box(f());
        best = best.min(start.elapsed());
    }
    (best, count)
}

fn main() {
    let lines: Vec<String> = (0..N)
        .map(|i| format!("INSERT INTO demo (id, val) VALUES ({i}, 'row-{i}');"))
        .collect();

    let (new_elapsed, new_count) = best_of(ROUNDS, || count_new(&lines));
    let (old_elapsed, old_count) = best_of(ROUNDS, || count_old(&lines));
    assert_eq!(new_count, old_count, "counts must match");

    let speedup = old_elapsed.as_secs_f64() / new_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("Phase 2 demo: counting {new_count} statements (best of {ROUNDS} rounds)");
    println!("  NEW (alloc-free count):   {new_elapsed:?}");
    println!("  OLD (Vec<String> alloc):  {old_elapsed:?}");
    println!("  speedup: {speedup:.2}x (per-statement String allocation removed from hot path)");
    if cfg!(debug_assertions) {
        println!(
            "  note: run with --release for the optimized hot-path number; \
             count_line's char iteration is only fully optimized then."
        );
    }
}
