mod alloc_fuzz {
    use bllist::cairnalloc::CairnAlloc;
    // Adapted from source code of bstack fuzz tests
    use bstack::{
        BStack, BStackSlice, BStackSliceAllocator, CheckedSlabBStackAllocator,
        FirstFitBStackAllocator,
    };
    use rand::{Rng, RngExt, SeedableRng, rngs::StdRng, rngs::ThreadRng};
    use std::{
        env, fs, io,
        ops::Range,
        path, process,
        sync::Mutex,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time,
    };

    // ── shared helpers ────────────────────────────────────────────────────────

    struct Guard(path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    enum Operation {
        Alloc(u64),
        Realloc(u64),
        Dealloc,
        Check,
        Reopen, // only used in run_reopen_fuzz
    }

    fn temp_path(prefix: &str) -> path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = process::id();
        env::temp_dir().join(format!("bllist_fuzz_{prefix}_{pid}_{id}.bstack"))
    }

    /// Fill `buf` with a deterministic pattern derived from `id` and `bias`.
    fn fill(buf: &mut [u8], id: u64, bias: u64) {
        let mut rng = StdRng::seed_from_u64(id ^ bias);
        rng.fill_bytes(buf);
    }

    /// Assert that `buf` matches the pattern for `id` and `bias`.
    fn check(buf: &[u8], id: u64, bias: u64, ctx: &str) {
        let mut expected = vec![0u8; buf.len()];
        fill(&mut expected, id, bias);
        if buf != &expected[..] {
            // Have a very nice message showing the exact byte offsets and values that differ,
            // with a hex dump of actual vs expected. Highlight differing bytes with brackets.
            let mut msg = format!(
                "{ctx}: data mismatch for id {id} under bias {bias}\n\
                offset   actual                                    expected\n\
                ──────── ───────────────────────────────────────── ────────────────────────────────────────"
            );

            for (chunk_idx, (actual_chunk, expected_chunk)) in
                buf.chunks(16).zip(expected.chunks(16)).enumerate()
            {
                let offset = chunk_idx * 16;
                let has_diff = actual_chunk != expected_chunk;

                let fmt_hex = |bytes: &[u8], highlight: bool| -> String {
                    let hex_part: String = bytes
                        .iter()
                        .enumerate()
                        .map(|(i, b)| {
                            let cell = format!("{b:02x}");
                            if highlight
                                && i < expected_chunk.len()
                                && bytes[i] != expected_chunk[i]
                            {
                                format!("[{cell}]") // bracket differing bytes
                            } else {
                                format!(" {cell} ")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("");
                    // Pad to fixed width (16 bytes × 3 chars each = 48 chars)
                    format!("{hex_part:<48}")
                };

                let marker = if has_diff { "!" } else { " " };
                msg.push_str(&format!(
                    "\n{marker}{offset:07x}  {}  {}",
                    fmt_hex(actual_chunk, true),
                    fmt_hex(expected_chunk, false),
                ));
            }

            panic!("{msg}");
        }
    }

    fn check_is_zero(buf: &[u8], ctx: &str) {
        if buf.iter().all(|&b| b == 0) {
            return;
        }

        let mut msg = format!(
            "{ctx}: buffer is not all zero\n\
         offset   data\n\
         ──────── ────────────────────────────────────────────────"
        );

        for (chunk_idx, chunk) in buf.chunks(16).enumerate() {
            let offset = chunk_idx * 16;
            let has_nonzero = chunk.iter().any(|&b| b != 0);

            let hex_part: String = chunk
                .iter()
                .map(|&b| {
                    if b != 0 {
                        format!("[{b:02x}]")
                    } else {
                        format!(" {b:02x} ")
                    }
                })
                .collect::<Vec<_>>()
                .join("");

            let marker = if has_nonzero { "!" } else { " " };
            msg.push_str(&format!("\n{marker}{offset:07x}  {hex_part:<48}"));
        }

        panic!("{msg}");
    }

    fn write_id<A: BStackSliceAllocator>(s: &BStackSlice<'_, A>, id: u64, bias: u64) {
        let mut buf = vec![0u8; s.len() as usize];
        fill(&mut buf, id, bias);
        s.write(&buf).unwrap();
    }

    fn verify_id<A: BStackSliceAllocator>(s: &BStackSlice<'_, A>, id: u64, bias: u64, ctx: &str) {
        check(&s.read().unwrap(), id, bias, ctx);
    }

    fn check_or_increment_counters(
        atomic_counter: &AtomicUsize,
        thread_counter: &mut usize,
        thread_limit: usize,
        total_limit: usize,
    ) -> bool {
        if *thread_counter >= thread_limit {
            return false;
        } else {
            *thread_counter += 1;
        }
        if atomic_counter.fetch_add(1, Ordering::SeqCst) >= total_limit {
            return false;
        }
        true
    }

    fn snapshot_bstack(
        path: path::PathBuf,
        lock: &Mutex<()>,
        thread_counter: usize,
        bias: u64,
        ctx: &str,
    ) {
        let _guard = lock.lock().unwrap();
        // Snapshot under fuzz/snapshot/ directory with a filename that includes the bias, thread_counter, and timestamp.
        let snapshot_dir = env::current_dir().unwrap().join("fuzz/snapshot");
        fs::create_dir_all(&snapshot_dir).unwrap();
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let snapshot_path = snapshot_dir.join(format!(
            "snapshot_{ctx}_T{bias}_C{thread_counter}({timestamp}).bstack"
        ));
        fs::copy(path, &snapshot_path).unwrap();
    }

    fn run_alloc_fuzz<A, M, G, H>(
        make: M,
        size_gen: G,
        io_error_handler: H,
        raw_io_mutex: &Mutex<()>,
        atomic_counter: &AtomicUsize,
        thread_limit: usize,
        total_limit: usize,
        snapshot_period: usize,
    ) where
        A: BStackSliceAllocator,
        M: Fn(BStack) -> io::Result<A>,
        G: Fn(&mut ThreadRng) -> Operation,
        H: Fn(io::Error),
    {
        let path = temp_path("ard");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();
        let mut rng = rand::rng();
        let thread_unique_bias = rng.next_u64();
        let mut live: Vec<(BStackSlice<'_, A>, u64)> = Vec::new();
        let mut thread_counter = 0usize;

        if thread_limit == 0 || total_limit == 0 {
            panic!(
                "incorrect testing configuration: thread_limit and total_limit must be greater than 0"
            );
        }

        loop {
            if thread_counter != 0 && thread_counter % snapshot_period == 0 {
                snapshot_bstack(
                    path.clone(),
                    raw_io_mutex,
                    thread_counter,
                    thread_unique_bias,
                    "fuzz",
                );
            }
            match size_gen(&mut rng) {
                Operation::Alloc(len) => {
                    if !check_or_increment_counters(
                        atomic_counter,
                        &mut thread_counter,
                        thread_limit,
                        total_limit,
                    ) {
                        break;
                    }
                    match alloc.alloc(len) {
                        Ok(s) => {
                            let id = rng.next_u64();
                            write_id(&s, id, thread_unique_bias);
                            live.push((s, id));
                        }
                        Err(e) => {
                            io_error_handler(e);
                        }
                    }
                }
                Operation::Realloc(new_len) => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = &mut live[idx];
                        let old_len = s.len();
                        match alloc.realloc(*s, new_len) {
                            Ok(s2) => {
                                verify_id(
                                    &s2.subslice(0, old_len),
                                    *id,
                                    thread_unique_bias,
                                    "fuzz - realloc",
                                );
                                check_is_zero(
                                    &s2.subslice(old_len, new_len).read().unwrap(),
                                    "fuzz - realloc",
                                );
                                write_id(&s2, *id, thread_unique_bias);
                                *s = s2;
                            }
                            Err(e) => {
                                io_error_handler(e);
                            }
                        }
                    }
                }
                Operation::Dealloc => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = live.swap_remove(idx);
                        verify_id(&s, id, thread_unique_bias, "fuzz - dealloc");
                        if let Err(e) = alloc.dealloc(s) {
                            io_error_handler(e);
                        }
                    }
                }
                Operation::Check => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = &live[idx];
                        verify_id(s, *id, thread_unique_bias, "fuzz - check");
                    }
                }
                _ => {
                    // Skip other operations
                }
            }
        }
    }

    fn run_reopen_fuzz<A, M, G, H>(
        make: M,
        size_gen: G,
        io_error_handler: H,
        raw_io_mutex: &Mutex<()>,
        atomic_counter: &AtomicUsize,
        thread_limit: usize,
        total_limit: usize,
        snapshot_period: usize,
    ) where
        A: BStackSliceAllocator,
        M: Fn(BStack) -> io::Result<A>,
        G: Fn(&mut ThreadRng) -> Operation,
        H: Fn(io::Error),
    {
        let path = temp_path("reopen");
        let _guard = Guard(path.clone());
        drop(make(BStack::open(&path).unwrap()).unwrap());

        let path = temp_path("ard");
        let _guard = Guard(path.clone());
        let mut alloc = make(BStack::open(&path).unwrap()).unwrap();
        let mut rng = rand::rng();
        let thread_unique_bias = rng.next_u64();
        let mut live: Vec<(Range<u64>, u64)> = Vec::new();
        let mut thread_counter = 0usize;

        if thread_limit == 0 || total_limit == 0 {
            panic!(
                "incorrect testing configuration: thread_limit and total_limit must be greater than 0"
            );
        }

        loop {
            if thread_counter != 0 && thread_counter % snapshot_period == 0 {
                snapshot_bstack(
                    path.clone(),
                    raw_io_mutex,
                    thread_counter,
                    thread_unique_bias,
                    "reopen",
                );
            }
            match size_gen(&mut rng) {
                Operation::Alloc(len) => {
                    if !check_or_increment_counters(
                        atomic_counter,
                        &mut thread_counter,
                        thread_limit,
                        total_limit,
                    ) {
                        break;
                    }
                    match alloc.alloc(len) {
                        Ok(s) => {
                            let id = rng.next_u64();
                            write_id(&s, id, thread_unique_bias);
                            live.push((s.range(), id));
                        }
                        Err(e) => {
                            io_error_handler(e);
                        }
                    }
                }
                Operation::Realloc(new_len) => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = &mut live[idx];
                        let old_len = s.end - s.start;
                        match alloc.realloc(
                            unsafe { BStackSlice::from_raw_parts(&alloc, s.start, old_len) },
                            new_len,
                        ) {
                            Ok(s2) => {
                                verify_id(
                                    &s2.subslice(0, old_len),
                                    *id,
                                    thread_unique_bias,
                                    "fuzz - realloc",
                                );
                                check_is_zero(
                                    &s2.subslice(old_len, new_len).read().unwrap(),
                                    "fuzz - realloc",
                                );
                                write_id(&s2, *id, thread_unique_bias);
                                *s = s2.range();
                            }
                            Err(e) => {
                                io_error_handler(e);
                            }
                        }
                    }
                }
                Operation::Dealloc => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = live.swap_remove(idx);
                        let s = unsafe {
                            BStackSlice::from_raw_parts(&alloc, s.start, s.end - s.start)
                        };
                        verify_id(&s, id, thread_unique_bias, "fuzz - dealloc");
                        if let Err(e) = alloc.dealloc(s) {
                            io_error_handler(e);
                        }
                    }
                }
                Operation::Check => {
                    if !live.is_empty() {
                        if !check_or_increment_counters(
                            atomic_counter,
                            &mut thread_counter,
                            thread_limit,
                            total_limit,
                        ) {
                            break;
                        }
                        let idx = rng.random_range(0..live.len());
                        let (s, id) = &live[idx];
                        let s = unsafe {
                            BStackSlice::from_raw_parts(&alloc, s.start, s.end - s.start)
                        };
                        verify_id(&s, *id, thread_unique_bias, "fuzz - check");
                    }
                }
                Operation::Reopen => {
                    if !check_or_increment_counters(
                        atomic_counter,
                        &mut thread_counter,
                        thread_limit,
                        total_limit,
                    ) {
                        break;
                    }
                    // First hold the mutex
                    let _guard = raw_io_mutex.lock().unwrap();
                    let stack = alloc.into_stack();
                    alloc = make(stack).unwrap();
                }
            }
        }
    }

    // This tests that attempting to free the same slice twice results in an error rather than panicking
    // or corrupting the allocator state. We test this by allocating three adjacent slices, freeing the
    // middle one, then reconstructing a handle to the same region and freeing it again.
    //
    // This is not strictly required by the API, since the slice-origin requirement already makes it UB
    // to construct a handle to the same region. However, it is beneficial for allocators provided by
    // this crate to handle this case gracefully and return an error rather than panic or corrupt state,
    // so we test it explicitly.
    fn run_double_free<A, F>(make: F)
    where
        A: BStackSliceAllocator,
        F: Fn(BStack) -> std::io::Result<A>,
    {
        let path = temp_path("dfree");
        let _guard = Guard(path.clone());
        let alloc = make(BStack::open(&path).unwrap()).unwrap();

        // Allocate a non-tail block by sandwiching it between two others.
        let before = alloc.alloc(64).unwrap();
        let target = alloc.alloc(64).unwrap();
        let after = alloc.alloc(64).unwrap();

        let (start, len) = (target.start(), target.len());
        alloc.dealloc(target).unwrap();

        // Reconstruct a handle to the same region and free it a second time.
        let again = unsafe { BStackSlice::from_raw_parts(&alloc, start, len) };
        let result = alloc.dealloc(again);
        assert!(result.is_err(), "double-free must return an error");

        alloc.dealloc(before).unwrap();
        alloc.dealloc(after).unwrap();
    }

    fn next_f64(rng: &mut ThreadRng) -> f64 {
        // Use upper 53 bits for f64 mantissa precision
        (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Samples from Gamma(shape=k, scale=1) using the Marsaglia-Tsang method.
    /// Returns a non-negative f64.
    fn sample_gamma_standard(rng: &mut ThreadRng, k: f64) -> f64 {
        assert!(k > 0.0, "shape k must be positive");

        if k < 1.0 {
            // Boost trick: Gamma(k) = Gamma(k+1) * U^(1/k)
            let u = next_f64(rng);
            return sample_gamma_standard(rng, k + 1.0) * u.powf(1.0 / k);
        }

        // Marsaglia-Tsang algorithm (works for k >= 1)
        let d = k - 1.0 / 3.0;
        let c = 1.0 / (9.0 * d).sqrt();

        loop {
            // Draw a standard normal via Box-Muller
            let u1 = next_f64(rng);
            let u2 = next_f64(rng);
            let x = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

            let v_raw = 1.0 + c * x;
            if v_raw <= 0.0 {
                continue;
            }
            let v = v_raw * v_raw * v_raw; // v = (1 + cx)^3

            let u = next_f64(rng);

            // Squeeze check (fast path)
            if u < 1.0 - 0.0331 * (x * x) * (x * x) {
                return d * v;
            }
            // Log check (slow path, rarely reached)
            if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
                return d * v;
            }
        }
    }

    /// Generates a u64 in [0, max] following a Gamma(k, theta) distribution.
    ///
    /// # Arguments
    /// * `seed` - a u64 as if obtained from rand's next_u64()
    /// * `max`  - upper bound of the output range
    /// * `k`    - shape parameter (k > 1 gives the 0-at-0, peak, decay shape)
    /// * `theta`- scale parameter (stretches the distribution)
    fn gamma_sample_u64(rng: &mut ThreadRng, max: u64, k: f64, theta: f64) -> u64 {
        assert!(k > 0.0, "shape must be positive");
        assert!(theta > 0.0, "scale must be positive");

        loop {
            // Sample from Gamma(k, theta)
            let sample = sample_gamma_standard(rng, k) * theta;

            // The gamma distribution has no hard upper bound, so we rejection-sample
            // values that fall outside [0, max]. For well-chosen k/theta this is rare.
            // Suggested heuristic: set theta so that the mode (k-1)*theta is near max/2.
            let scaled = sample.round() as i64;
            if scaled >= 0 && scaled <= max as i64 {
                return scaled as u64;
            }
        }
    }

    // Actual fuzz
    #[test]
    fn fuzz_alloc_cairnalloc() {
        let raw_io_mutex = Mutex::new(());
        let atomic_counter = AtomicUsize::new(0);
        run_alloc_fuzz(
            |stack| CairnAlloc::new(stack),
            |rng| {
                let op = rng.random_range(0..100);
                if op < 40 {
                    Operation::Alloc(gamma_sample_u64(rng, 65536, 2.0, 64.0))
                } else if op < 70 {
                    Operation::Realloc(gamma_sample_u64(rng, 65536, 2.0, 64.0))
                } else if op < 90 {
                    Operation::Dealloc
                } else {
                    Operation::Check
                }
            },
            |e| eprintln!("I/O error during fuzzing: {e}"),
            &raw_io_mutex,
            &atomic_counter,
            10,   // thread_limit
            1000, // total_limit
            100,  // snapshot_period
        );
    }

    #[test]
    fn fuzz_reopen_cairnalloc() {
        let raw_io_mutex = Mutex::new(());
        let atomic_counter = AtomicUsize::new(0);
        run_reopen_fuzz(
            |stack| CairnAlloc::new(stack),
            |rng| {
                let op = rng.random_range(0..100);
                if op < 35 {
                    Operation::Alloc(gamma_sample_u64(rng, 65536, 2.0, 64.0))
                } else if op < 65 {
                    Operation::Realloc(gamma_sample_u64(rng, 65536, 2.0, 64.0))
                } else if op < 85 {
                    Operation::Dealloc
                } else if op < 95 {
                    Operation::Check
                } else {
                    Operation::Reopen
                }
            },
            |e| eprintln!("I/O error during reopen fuzzing: {e}"),
            &raw_io_mutex,
            &atomic_counter,
            10,   // thread_limit
            1000, // total_limit
            100,  // snapshot_period
        );
    }

    #[test]
    fn double_free_cairnalloc() {
        run_double_free(|stack| CairnAlloc::new(stack));
    }
}
