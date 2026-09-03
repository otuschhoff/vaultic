# Phase 21: Crawl optimization with `cwalk` and `pathdiff`

[← Back to roadmap index](00-overview.md)

[← Phase 20](phase-20-quorum-based-encryption-unlock.md) · [Phase 22 →](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md)

**Goal:** accelerate backup crawls across 1.5+ billion inode storage targets using `cwalk` concurrent directory traversal and `pathdiff` selective change-path crawling with guaranteed event coverage and SVM/volume topology mapping.

**Implementation steps:**

1. Integrate `cwalk` (`github.com/otuschhoff/cwalk`) into the archiver and reconciliation scanner pipeline, replacing sequential traversal with parallel, multi-threaded directory walking with configurable concurrency (`--cwalk-concurrency N`) and queue capacity bounds.
2. Import `pathdiff` (`github.com/otuschhoff/pathdiff`) into `vaultic` (e.g. `internal/pathdiff`), making in-scope enhancements to support volume ID-to-name resolution, target host LIF (Logical Interface) $\rightarrow$ SVM (Storage Virtual Machine) $\rightarrow$ volume mapping, and service-owned observation continuity.
3. Implement 100% event-coverage verification: query the running `pathdiff` service for changes in the source path and parent-snapshot-to-backup-start window, together with the time from which that resolved path's LIF/SVM observation has been continuous. Reject late observation, reconnects, retention gaps, or unmonitored windows.
4. Implement selective change-path crawl execution: if 100% event coverage is verified, crawl only the modified subtrees identified by `pathdiff`; if event coverage is incomplete or unverified, fall back automatically to a full `cwalk` traversal.
5. Expose CLI crawl options: `--use-cwalk`, `--cwalk-concurrency N`, `--use-pathdiff`, `--pathdiff-endpoint`, `--pathdiff-require-coverage`, and `--pathdiff-svm-map`.

**Tests:** `cwalk` high-concurrency directory traversal correctness test comparing results against standard traversal; `pathdiff` volume ID resolution and target host LIF $\rightarrow$ SVM $\rightarrow$ volume topology matching test; event coverage gap detection test verifying automatic fallback to full `cwalk` scan when event logs are truncated; selective change-path crawl integration benchmark demonstrating subtree skipping when changes are sparse; imported `pathdiff` module unit tests.

**Exit criterion:** backup crawls achieve linear scaling with `cwalk` concurrency, selective `pathdiff` crawls skip unchanged subtrees when 100% event coverage is verified, and any coverage gap or topology mismatch falls back safely to a full `cwalk` crawl.

## Implementation status

Implemented in the Go backup pipeline:

- Full archiver discovery uses upstream `github.com/otuschhoff/cwalk` for plain local filesystems when `--use-cwalk` is set. Directory results flow through a bounded 4096-entry queue into fixed-size Pebble batches and are consumed during deterministic tree construction. Selective runs cwalk only below collapsed changed roots; unsupported filesystem implementations retain direct sequential directory reads.
- Upstream `github.com/otuschhoff/pathdiff` supplies the localhost control-socket client and event, engine, status, retention, volume, SVM, and LIF models. `internal/crawl` exposes a transport-neutral change-service query and derives a continuous observation window by checking the same matching engine session before and after each path-window query.
- A verified selective plan reuses unchanged directory nodes and subtree IDs directly from the parent snapshot. Changed-path ancestors are still listed, so file creation, deletion, and rename events rebuild the containing directory deterministically.
- A cwalk or selective run does not launch a redundant full progress scanner, and a selective run does not produce an authoritative metadata-crawl claim. Reused descendants were not independently observed.
- Any missing parent, unsupported filesystem, endpoint failure, disabled or insufficient retention, absent engine, incomplete topology, observation start after the parent, or engine reconnect during the query selects a full crawl. `--pathdiff-require-coverage` converts that fallback into a fatal pre-snapshot error.

Validation includes sequential/parallel scanner equivalence, race detection, LIF/SVM/volume resolution, volume ID matching, late-observation/reconnect/retention fallback, selective parent-subtree reuse, CLI contract tests, and worker/sparse-selection benchmarks.

The linear-scaling exit criterion remains hardware-dependent. `BenchmarkScannerCWalkConcurrency` provides the repeatable 1/2/4/8-worker measurement. The adapter starts one worker and uses cwalk's live resize API after root discovery, avoiding the upstream stagger delay for additional workers. Release qualification must run the benchmark against the target storage because local SSD, NFS server, network, and metadata-cache behavior determine the scaling curve.
