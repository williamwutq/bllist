# Planned Features

This document outlines upcoming features planned for the `bllist` crate. These enhancements aim to improve usability, performance, and integration while maintaining the core principles of durability, crash-safety, and checksum integrity.

## 1. Iterators and Traversal APIs

**Description**: Add iterator methods (`iter()`, `iter_mut()`) to allow read-only or mutable traversal of the list without consuming elements.

**Rationale**: Enables efficient searching, filtering, and bulk operations on large lists without the overhead of popping elements.

**Implementation Notes**:
- Implement `Iterator` and `DoubleEndedIterator` traits for both `FixedBlockList` and `DynamicBlockList`.
- Use zero-copy reads where possible to avoid unnecessary allocations.
- Ensure thread-safety for concurrent reads.

## 2. Random Access or Indexing

**Description**: Implement indexing operations like `get(index)` and `set(index, data)` for O(n) access to elements by position.

**Rationale**: Supports use cases requiring array-like access, such as queues or priority structures, beyond the current FIFO model.

**Implementation Notes**:
- Traverse the linked list to reach the desired index.
- For `set`, handle block updates with checksum recalculation and durable writes.
- Add bounds checking to prevent out-of-range access.

**Potential Challenges**:
- Performance for large indices; consider caching or alternative data structures.
- Ensuring atomicity for updates in crash-safe scenarios.

## 3. Compression Options

**Description**: Add optional block compression (e.g., via zstd or lz4) to reduce disk usage for large payloads, with configurable thresholds.

**Rationale**: Minimizes storage footprint for data-heavy applications, improving efficiency without sacrificing durability.

**Implementation Notes**:
- Integrate compression libraries as optional dependencies.
- Compress payloads before writing; decompress on read.
- Add configuration options for compression level and minimum block size.

## 4. Async I/O Integration

**Description**: Provide async versions of core operations (e.g., `async_push_front`, `async_pop_front`) using Tokio.

**Rationale**: Enables non-blocking I/O in async applications, improving performance in concurrent environments.

**Implementation Notes**:
- Use async file I/O primitives for reads/writes.
- Maintain crash-safety with async fsync equivalents.
- Add feature flags for async support.
