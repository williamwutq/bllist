#[allow(unused)]
use bllist::cairnalloc::CairnAlloc;
use bllist::use_sims::gamma::gamma_sample_u64;
use bstack::{
    BStack, BStackSlice, BStackSliceAllocator, CheckedSlabBStackAllocator, FirstFitBStackAllocator,
};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    env,
    fmt::Debug,
    fs, io, path, process,
    sync::{
        Barrier,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

struct Guard(path::PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn temp_path(prefix: &str) -> path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = process::id();
    env::temp_dir().join(format!("bllist_bench_{prefix}_{pid}_{id}.bstack"))
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Operation {
    Alloc,
    Realloc,
    Dealloc,
}

#[allow(unused)]
fn default_opgen(rng: &mut StdRng) -> Operation {
    // 30% alloc, 50% realloc, 20% dealloc
    let op = rng.random_range(0..10);
    match op {
        0..=2 => Operation::Alloc,
        3..=7 => Operation::Realloc,
        8..=9 => Operation::Dealloc,
        _ => unreachable!(),
    }
}

#[allow(unused)]
fn flat_opgen(rng: &mut StdRng) -> Operation {
    let op = rng.random_range(0..3);
    match op {
        0 => Operation::Alloc,
        1 => Operation::Realloc,
        2 => Operation::Dealloc,
        _ => unreachable!(),
    }
}

#[allow(unused)]
fn grow_opgen(rng: &mut StdRng) -> Operation {
    let op = rng.random_range(0..10);
    match op {
        0..=5 => Operation::Alloc,
        6..=7 => Operation::Realloc,
        8..=9 => Operation::Dealloc,
        _ => unreachable!(),
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum BenchDebugLevel {
    Panic,
    None,
    Fast,
    Full,
    Extra,
}

const PRE_ALLOC_COUNT: usize = 200;

#[allow(dead_code)]
fn print_entire_bstack(stack: &BStack) {
    let mut offset = 0u64;
    let mut reader = stack.reader();
    loop {
        use std::io::Read;
        let mut buf = [0u8; 16];
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let hex: String = buf[..n]
                    .iter()
                    .enumerate()
                    .map(|(i, b)| {
                        if i == 8 {
                            format!(" {:02x}", b)
                        } else {
                            format!("{:02x}", b)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // hex width: 3n-1 for n≤8 (no group gap), 3n for n>8 (group gap adds 1)
                let hex_width = if n <= 8 { 3 * n - 1 } else { 3 * n };
                let pad = " ".repeat(48 - hex_width);
                let interp: String = [0usize, 8]
                    .iter()
                    .filter(|&&start| start < n)
                    .map(|&start| {
                        let end = (start + 8).min(n);
                        let chunk = &buf[start..end];
                        if chunk.len() == 8 {
                            let val = u64::from_le_bytes(chunk.try_into().unwrap());
                            format!("{val:>20}")
                        } else {
                            let mut arr = [0u8; 8];
                            arr[..chunk.len()].copy_from_slice(chunk);
                            let val = u64::from_le_bytes(arr);
                            format!("{val:>20}?")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("  ");
                eprintln!("{offset:08x}  {hex}{pad}  |{interp}|");
                offset += n as u64;
            }
            Err(e) => {
                eprintln!("Error reading BStack at offset {offset:08x}: {e}");
                break;
            }
        }
    }
}

fn bench_allocator<A, M, OG, SG>(
    c: &mut Criterion,
    debug_level: BenchDebugLevel,
    thread_count: usize,
    group_name: &str,
    bench_name: &str,
    make: M,
    op_gen: OG,
    size_gen: SG,
) where
    A: BStackSliceAllocator + Sync,
    M: Fn(BStack) -> std::io::Result<A>,
    OG: Fn(&mut StdRng) -> Operation,
    SG: Fn(&mut StdRng) -> u64,
{
    assert!(thread_count >= 1, "thread_count must be at least 1");
    let mut group = c.benchmark_group(group_name);
    group.bench_function(bench_name, |b| {
        b.iter_custom(|iters| {
            // Setup (not timed)
            let path = temp_path("bench");
            let _guard = Guard(path.clone());
            let alloc = make(BStack::open(&path).unwrap()).unwrap();
            let mut rng = StdRng::seed_from_u64(48);

            if debug_level == BenchDebugLevel::Extra || debug_level == BenchDebugLevel::Full {
                eprintln!("Benchmarking {bench_name} with {iters} iterations across {thread_count} thread(s)");
                eprintln!("Parameters:");
                eprintln!("  Threads: {thread_count}");
                eprintln!("  Pre-allocations: {PRE_ALLOC_COUNT}");
                eprintln!("  Iterations: {iters}");
                eprintln!("  Random seed: 48");
                eprintln!("  Rng: StdRng {:?}", rng);
                if debug_level == BenchDebugLevel::Extra {
                    use std::any::type_name;
                    eprintln!("  Operation generator: {:?}", type_name::<OG>());
                    eprintln!("  Size generator: {:?}", type_name::<SG>());
                }
                eprintln!("Caching {PRE_ALLOC_COUNT} pre-allocation decisions");
                eprintln!("Calling operation generator {iters} times");
            }

            // Cache decisions (not timed), then split across threads
            let all_ops: Vec<Operation> = (0..iters).map(|_| op_gen(&mut rng)).collect();
            if debug_level == BenchDebugLevel::Extra || debug_level == BenchDebugLevel::Full {
                let total = PRE_ALLOC_COUNT as u64 + iters;
                eprintln!("Calling size generator {total} times");
            }
            let all_pre_sizes: Vec<u64> =
                (0..PRE_ALLOC_COUNT).map(|_| size_gen(&mut rng)).collect();
            let all_sizes: Vec<u64> = (0..iters as usize).map(|_| size_gen(&mut rng)).collect();

            let iters_per_thread = iters as usize / thread_count;
            let pre_per_thread = PRE_ALLOC_COUNT / thread_count;

            // Build per-thread (pre_sizes, ops, sizes); last thread absorbs remainders
            let thread_data: Vec<(Vec<u64>, Vec<Operation>, Vec<u64>)> = (0..thread_count)
                .map(|tid| {
                    let pre_start = tid * pre_per_thread;
                    let pre_end = if tid == thread_count - 1 {
                        PRE_ALLOC_COUNT
                    } else {
                        pre_start + pre_per_thread
                    };
                    let op_start = tid * iters_per_thread;
                    let op_end = if tid == thread_count - 1 {
                        iters as usize
                    } else {
                        op_start + iters_per_thread
                    };
                    (
                        all_pre_sizes[pre_start..pre_end].to_vec(),
                        all_ops[op_start..op_end].to_vec(),
                        all_sizes[op_start..op_end].to_vec(),
                    )
                })
                .collect();

            if debug_level == BenchDebugLevel::Extra || debug_level == BenchDebugLevel::Full {
                eprintln!("Pre-allocating {PRE_ALLOC_COUNT} slices across {thread_count} thread(s)");
            }

            if debug_level == BenchDebugLevel::Extra {
                if let Ok(len) = alloc.stack().len() {
                    eprintln!("Initial BStack length: {}", len);
                } else {
                    eprintln!("Initial BStack length: unknown (error reading length)");
                }
                eprintln!("Initial BStack dump:");
                print_entire_bstack(alloc.stack());
                eprintln!("");
            }

            if debug_level == BenchDebugLevel::Extra || debug_level == BenchDebugLevel::Full {
                eprintln!("Starting timed benchmark with {iters} operations across {thread_count} thread(s)");
            }

            // Barrier ensures all threads finish pre-allocation before any starts timing
            let barrier = Barrier::new(thread_count);

            let elapsed = std::thread::scope(|s| {
                let handles: Vec<_> = thread_data
                    .into_iter()
                    .enumerate()
                    .map(|(tid, (pre_sizes, ops, sizes))| {
                        let alloc = &alloc;
                        let barrier = &barrier;
                        s.spawn(move || {
                            let mut live: Vec<BStackSlice<'_, A>> =
                                Vec::with_capacity(pre_sizes.len());
                            // Each thread uses a distinct seed so index choices don't correlate
                            let mut rng = StdRng::seed_from_u64(48 + tid as u64);
                            let mut failed_allocs = Vec::<(u64, io::Error)>::new();

                            // Pre-populate live allocations (not timed)
                            for len in pre_sizes {
                                match alloc.alloc(len) {
                                    Ok(s) => live.push(s),
                                    Err(e) => match debug_level {
                                        BenchDebugLevel::Panic => {
                                            panic!("Pre-allocation failed: {e:?}")
                                        }
                                        BenchDebugLevel::None => {}
                                        BenchDebugLevel::Fast => eprintln!(
                                            "[thread {tid}] Pre-allocation failed while attempting to allocate {len} bytes: {e:?}"
                                        ),
                                        _ => {
                                            failed_allocs.push((len, e));
                                        }
                                    },
                                }
                            }

                            if debug_level == BenchDebugLevel::Full {
                                if !failed_allocs.is_empty() {
                                    eprintln!(
                                        "[thread {tid}] {} pre-allocations failed:",
                                        failed_allocs.len()
                                    );
                                    for (len, e) in &failed_allocs {
                                        eprintln!("  Failed to allocate {len} bytes: {e:?}");
                                    }
                                }
                            } else if debug_level == BenchDebugLevel::Extra {
                                eprintln!("[thread {tid}] Pre-allocated {} slices", live.len());
                                if !failed_allocs.is_empty() {
                                    eprintln!(
                                        "[thread {tid}] {} pre-allocations failed:",
                                        failed_allocs.len()
                                    );
                                    for (len, e) in &failed_allocs {
                                        eprintln!("  Failed to allocate {len} bytes: {e:?}");
                                    }
                                }
                            }

                            // Wait for all threads to finish pre-allocation before timing starts
                            barrier.wait();

                            // Timed measurement
                            let start = Instant::now();
                            let mut old_slice = BStackSlice::empty(alloc);
                            for (len, op) in sizes.into_iter().zip(ops.into_iter()) {
                                let res: Result<(), io::Error> = match op {
                                    Operation::Alloc => alloc.alloc(len).map(|s| {
                                        live.push(s);
                                    }),
                                    Operation::Realloc => if live.is_empty() {
                                        alloc.alloc(len)
                                    } else {
                                        let slice =
                                            live.swap_remove(rng.random_range(0..live.len()));
                                        if debug_level == BenchDebugLevel::Extra {
                                            old_slice = slice;
                                        }
                                        alloc.realloc(slice, len)
                                    }
                                    .map(|s| {
                                        live.push(s);
                                    }),
                                    Operation::Dealloc => {
                                        if live.is_empty() {
                                            if let Ok(slice) = alloc.alloc(len) {
                                                Ok(live.push(slice))
                                            } else {
                                                Ok(())
                                            }
                                        } else {
                                            let slice =
                                                live.swap_remove(rng.random_range(0..live.len()));
                                            if debug_level == BenchDebugLevel::Extra {
                                                old_slice = slice;
                                            }
                                            alloc.dealloc(slice)
                                        }
                                    }
                                };

                                if debug_level == BenchDebugLevel::Panic {
                                    res.expect("Allocation operation failed");
                                } else if debug_level == BenchDebugLevel::None {
                                    // Ignore errors
                                } else if debug_level == BenchDebugLevel::Fast
                                    || debug_level == BenchDebugLevel::Full
                                {
                                    if let Err(e) = res {
                                        match op {
                                            Operation::Alloc => eprintln!(
                                                "[thread {tid}] Allocation failed while attempting to allocate {len} bytes: {e:?}"
                                            ),
                                            Operation::Realloc => eprintln!(
                                                "[thread {tid}] Reallocation failed while attempting to reallocate to {len} bytes: {e:?}"
                                            ),
                                            Operation::Dealloc => eprintln!(
                                                "[thread {tid}] Deallocation failed: {e:?}"
                                            ),
                                        }
                                        if debug_level == BenchDebugLevel::Fast {
                                            break;
                                        }
                                    }
                                } else if debug_level == BenchDebugLevel::Extra {
                                    if let Err(e) = res {
                                        match op {
                                            Operation::Alloc => eprintln!(
                                                "[thread {tid}] Allocation failed while attempting to allocate {len} bytes: {e:?}"
                                            ),
                                            Operation::Realloc => {
                                                let old_len = old_slice.len();
                                                let offset = old_slice.start();
                                                eprintln!(
                                                    "[thread {tid}] Reallocation failed while attempting to reallocate from {old_len} bytes to {len} bytes at offset {offset}: {e:?}"
                                                )
                                            }
                                            Operation::Dealloc => {
                                                let old_len = old_slice.len();
                                                let offset = old_slice.start();
                                                eprintln!(
                                                    "[thread {tid}] Deallocation failed for slice of length {old_len} at offset {offset}: {e:?}"
                                                )
                                            }
                                        }
                                        if tid == 0 {
                                            eprintln!("Current BStack dump:");
                                            print_entire_bstack(alloc.stack());
                                            eprintln!("");
                                        }
                                    }
                                }
                            }
                            start.elapsed()
                        })
                    })
                    .collect();

                // Wall-clock time is the slowest thread
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .max()
                    .unwrap_or_default()
            });

            if debug_level == BenchDebugLevel::Extra || debug_level == BenchDebugLevel::Full {
                eprintln!("Benchmark completed in {:?}", elapsed);
                if let Ok(len) = alloc.stack().len() {
                    eprintln!("Final BStack length: {}", len);
                } else {
                    eprintln!("Final BStack length: unknown (error reading length)");
                }
                if debug_level == BenchDebugLevel::Extra {
                    eprintln!("Final BStack dump:");
                    print_entire_bstack(alloc.stack());
                    eprintln!("");
                }
            }

            // Drop live then alloc, guard removes file (not timed)
            elapsed
        });
    });
    group.finish();
}

fn bench_alloc(c: &mut Criterion) {
    // bench_allocator(c, BenchDebugLevel::Panic, 1, "alloc_mixed", "CairnAlloc", |stack| {
    //     CairnAlloc::new(stack)
    // }, default_opgen, |rng| gamma_sample_u64(rng, 1024, 2.0, 1.0));
    bench_allocator(
        c,
        BenchDebugLevel::Panic,
        1,
        "alloc_mixed",
        "FirstFitBStackAllocator",
        FirstFitBStackAllocator::new,
        default_opgen,
        |rng| gamma_sample_u64(rng, 1024, 2.0, 1.0),
    );
    bench_allocator(
        c,
        BenchDebugLevel::Panic,
        1,
        "alloc_mixed",
        "CheckedSlabBStackAllocator_48",
        |stack| CheckedSlabBStackAllocator::new(stack, 48),
        default_opgen,
        |rng| gamma_sample_u64(rng, 1024, 2.0, 1.0),
    );
    bench_allocator(
        c,
        BenchDebugLevel::Panic,
        1,
        "alloc_mixed",
        "CheckedSlabBStackAllocator_64",
        |stack| CheckedSlabBStackAllocator::new(stack, 64),
        default_opgen,
        |rng| gamma_sample_u64(rng, 1024, 2.0, 1.0),
    );
    bench_allocator(
        c,
        BenchDebugLevel::Panic,
        1,
        "alloc_mixed",
        "CheckedSlabBStackAllocator_128",
        |stack| CheckedSlabBStackAllocator::new(stack, 128),
        default_opgen,
        |rng| gamma_sample_u64(rng, 1024, 2.0, 1.0),
    );
}

criterion_group!(benches, bench_alloc);
criterion_main!(benches);
