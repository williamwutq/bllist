mod alloc_fuzz {
    use bllist::cairnalloc::CairnAlloc;
    use bllist::use_sims::gamma::gamma_sample_u64;
    // Adapted from source code of bstack fuzz tests
    use bstack::{BStack, BStackSlice, BStackSliceAllocator};
    use rand::{Rng, RngExt, SeedableRng, rngs::StdRng, rngs::ThreadRng};
    use std::{
        env, fs, io,
        io::Read,
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

    #[allow(dead_code)]
    fn print_entire_bstack(stack: &BStack) {
        let mut offset = 0u64;
        println!("Entire BStack contents:");
        let mut reader = stack.reader();
        loop {
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
                    println!("{offset:08x}  {hex}{pad}  |{interp}|");
                    offset += n as u64;
                }
                Err(e) => {
                    eprintln!("Error reading BStack at offset {offset:08x}: {e}");
                    break;
                }
            }
        }
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
            print_entire_bstack(&alloc.stack()); // for debugging: see entire stack state in fuzz logs
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
                                // Verify preserved prefix (min of old and new length).
                                let preserved = old_len.min(new_len);
                                verify_id(
                                    &s2.subslice(0, preserved),
                                    *id,
                                    thread_unique_bias,
                                    "fuzz - realloc",
                                );
                                // Only check zero-extension when the allocation grew.
                                if new_len > old_len {
                                    check_is_zero(
                                        &s2.subslice(old_len, new_len).read().unwrap(),
                                        "fuzz - realloc",
                                    );
                                }
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
        // In debug builds the allocator panics with the error message instead of returning Err.
        let again = unsafe { BStackSlice::from_raw_parts(&alloc, start, len) };
        #[cfg(debug_assertions)]
        {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| alloc.dealloc(again)));
            match outcome {
                Err(payload) => {
                    let msg = payload
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| payload.downcast_ref::<&str>().copied())
                        .unwrap_or("");
                    assert!(
                        msg.to_lowercase().contains("double free"),
                        "double-free panic message must mention \"double free\", got: {msg:?}"
                    );
                }
                Ok(_) => panic!("double-free must panic in debug builds"),
            }
        }
        #[cfg(not(debug_assertions))]
        {
            let result = alloc.dealloc(again);
            let err = result.expect_err("double-free must return an error");
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("double free"),
                "double-free error message must mention \"double free\", got: {msg:?}"
            );
        }

        alloc.dealloc(before).unwrap();
        alloc.dealloc(after).unwrap();
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
