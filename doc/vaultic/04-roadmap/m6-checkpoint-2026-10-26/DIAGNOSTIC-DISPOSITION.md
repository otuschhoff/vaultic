# Full-Catalog Equal-Work Diagnostic: Not Promoted

The underlying diagnostic passed. The material-throughput gate did not pass.
The outer exit status 1 is the intentional fail-closed gate rejection, not a
test timeout or a failure to establish the complete catalog. No new RAM, KV,
auto, check, init or backup workflow was started. Full M6 acceptance is unmet.

## Verified Scope And Safety

The fresh diagnostic populated all 10,170 indexes, 419,853 packs and
380,166,821 blobs, inspected all 143 snapshot roots, and replayed one identical
512-tree, 35,038-node corpus. Each pipeline round wrote 70,076 debts in fresh
namespaces. All canonical node digests matched and all 45 selected native debt
key/value samples matched after reopen. These samples are not a full integrity
check. Catalog population does not establish that every native page was in RAM.

The diagnostic completed in 7,676.996 seconds with independently sampled combined
CLI/native RSS peaking at 59,748,937,728 bytes, below the unchanged 200-GB ceiling.
The 30,324 RSS records reconcile exactly with that peak. The independent observer
retained 1,534 records from before source preload through the last measurements;
its largest interval was 5.030 seconds, with no unavailable-process observations.
The workload unit is inactive with no owned CLI/native/controller/observer
survivors. Production PID 522 and its pinned executable hash remained unchanged.
The original metadata inventory, all 52 sources, exclusions and limits were
reverified. All 41 launch inputs remain unchanged. Preexisting main-checkout
changes, including schema-store changes observed after capture, were not edited.

## Equal-Work Results

| Pair | Pipeline Speedup | Loader Speedup |
| --- | ---: | ---: |
| 1 | 1.0967x | 1.2056x |
| 2 | 1.0887x | 1.4337x |
| 3 | 1.1009x | 1.1978x |
| 4 | 0.9919x | 2.4393x |
| 5 | 1.0970x | 1.2147x |

Median paired pipeline speedup was 1.0967x, below the required 1.25x. One pair
regressed; none reached 1.20x. Median loader speedup was 1.2147x, below the
required 1.50x. The single-file cache microbenchmark therefore did not establish
a material gain on this full-catalog component pipeline. Do not lower the gate
or extrapolate its microbenchmark gain into a full import speedup.

Source preload took 2,602.440 seconds; native catalog preparation took
5,033.229 seconds. Per 35,038-node round, median pure JSON decoding was
0.22375 seconds, decoding plus diagnostic fact hashing/debt encoding was
0.55819 seconds, and writing 70,076 native mutations was 0.55063 seconds.
The loader component read 53,685,387 plaintext bytes in 1,536 calls per round.
Decoded bytes are not physical NFS I/O. Decoding is included in the fact/encoding
measurement: these numbers must not be summed as exclusive wall partitions.
The combined pipeline also includes diagnostic hashing beyond ordinary import.

## Disposition

Retain the source, binaries, test gates and negative evidence. Do not promote
the scoped-descriptor candidate or spend another full-length import run on it.
No failed database is reused. The existing full-RAM workflow remains blocked;
no KV/auto matrix or independent backup is authorized by a failed gate.

Further throughput work should discriminate native submission/insertion and
real debt encoding against representative traversal work, rather than another
cache heuristic. The short warm breadth-first corpus and shared storage/cache
limit generalization; five pairs are not proof about every subtree or snapshot.
Any new repair needs a separate pin and the same material equal-work gate.

Post-capture review found that the historical controller checks final observer
liveness after gate persistence. A separately sealed `nextsteps-v2.py` fixes
that positive path: it checks observer liveness and complete measurement coverage
before gate persistence, and requires a hash-bound completion receipt created
only after observer cleanup before allowing RAM acceptance. All nine controller
tests pass, including missing/gapped/wrong-identity observations and changed or
incomplete receipts. Its checks also validate this run's actual observer coverage.
The historical sealed controller and capture remain unchanged. The revision
does not create a positive receipt or make this negative candidate eligible.