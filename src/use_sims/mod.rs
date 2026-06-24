//! Use-case simulation utilities for benchmarking and fuzzing.
//!
//! This module provides statistical distributions and workload generators
//! used by the crate's criterion benchmarks and fuzz test suites to produce
//! realistic allocation size sequences.

pub mod gamma;
