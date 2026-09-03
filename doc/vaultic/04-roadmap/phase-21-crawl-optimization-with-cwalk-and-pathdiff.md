# Phase 21: Crawl optimization with `cwalk` and `pathdiff`

[← Back to roadmap index](00-overview.md)

[← Phase 20](phase-20-quorum-based-encryption-unlock.md) · [Phase 22 →](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md)

**Goal:** accelerate backup crawls across 1.5+ billion inode storage targets using `cwalk` concurrent directory traversal and `pathdiff` selective change-path crawling with guaranteed event coverage and SVM/volume topology mapping.

**Implementation steps:**

1. Integrate `cwalk` (`github.com/otuschhoff/cwalk`) into the archiver and reconciliation scanner pipeline, replacing sequential traversal with parallel, multi-threaded directory walking with configurable concurrency (`--cwalk-concurrency N`) and queue capacity bounds.
2. Import `pathdiff` (`github.com/otuschhoff/pathdiff`) into `vaultic` (e.g. `internal/pathdiff`), making in-scope enhancements to support volume ID-to-name resolution, target host LIF (Logical Interface) $\rightarrow$ SVM (Storage Virtual Machine) $\rightarrow$ volume mapping, and event sequence continuity checks.
3. Implement 100% event-coverage verification: query `pathdiff` for contiguous change events since the last snapshot timestamp of the source path; verify zero sequence gaps, buffer overflows, or unmonitored windows.
4. Implement selective change-path crawl execution: if 100% event coverage is verified, crawl only the modified subtrees identified by `pathdiff`; if event coverage is incomplete or unverified, fall back automatically to a full `cwalk` traversal.
5. Expose CLI crawl options: `--use-cwalk`, `--cwalk-concurrency N`, `--use-pathdiff`, `--pathdiff-endpoint`, `--pathdiff-require-coverage`, and `--pathdiff-svm-map`.

**Tests:** `cwalk` high-concurrency directory traversal correctness test comparing results against standard traversal; `pathdiff` volume ID resolution and target host LIF $\rightarrow$ SVM $\rightarrow$ volume topology matching test; event coverage gap detection test verifying automatic fallback to full `cwalk` scan when event logs are truncated; selective change-path crawl integration benchmark demonstrating subtree skipping when changes are sparse; imported `pathdiff` module unit tests.

**Exit criterion:** backup crawls achieve linear scaling with `cwalk` concurrency, selective `pathdiff` crawls skip unchanged subtrees when 100% event coverage is verified, and any coverage gap or topology mismatch falls back safely to a full `cwalk` crawl.
