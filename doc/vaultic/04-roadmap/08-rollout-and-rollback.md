# Rollout and Rollback

[← Back to roadmap index](00-overview.md)

## 18. Rollout and rollback

1. Ship the legacy adapter and detection code disabled by default.
2. Enable schema and import commands for operators without making SlateDB
   authoritative.
3. Run import plus `index check` and inspect crawl debt.
4. Run one backup crawl in legacy-authoritative mode while populating SlateDB.
5. Compare exported JSON with the original JSON index set.
6. Enable SlateDB authority for a test repository with synchronous JSON export.
7. Expand by repository size and backend type only after recovery tests pass.
8. Keep a rollback command that stops SlateDB writes, preserves the SlateDB
   namespace, and resumes from legacy JSON indexes.
9. Do not remove legacy JSON indexes until a separately approved migration
   policy exists.
10. Introduce a new backend by declaring it in the registry and letting the
   scheduler converge, verifying with `index placement` and
   `index backends --compare` before any policy depends on it. Never begin
   evicting from an existing backend until the new one reports its placements
   live and the durability predicate holds without it.

