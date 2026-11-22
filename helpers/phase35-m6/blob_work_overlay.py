"""Install the blob MultiGet diagnostic into isolated source copies only."""

import argparse
from pathlib import Path
import subprocess


REVISION = "43a562e3637d3d9ce12bec82a5b9e3deb47843fa"


def install(engine, native):
    roots = [root.resolve() for root in (engine, native)]
    if any(not root.is_relative_to(Path('/run')) for root in roots):
        raise ValueError('diagnostic sources must be private copies under /run')
    revision = subprocess.check_output(
        ['git', '-C', str(engine), 'rev-parse', 'HEAD'], text=True).strip()
    if revision != REVISION or subprocess.check_output(
            ['git', '-C', str(engine), 'status', '--porcelain'], text=True):
        raise ValueError('expected a clean checkout of the pinned SlateDB revision')
    pending = {}

    def replace(path, old, new):
        source = pending.get(path)
        if source is None:
            source = path.read_text()
        if source.count(old) != 1:
            raise ValueError(f'{path}: expected exactly one matching anchor: {old}')
        pending[path] = source.replace(old, new)

    crate = engine / 'slatedb/src'
    replace(crate / 'lib.rs', 'mod multi_sst;', 'mod multi_sst;\npub mod blob_work;')
    observer = crate / 'blob_work.rs'
    if observer.exists():
        raise ValueError(f'already exists: {observer}')
    pending[observer] = '''//! Diagnostic work counters scoped to one inline MultiGet future.
use std::{cell::Cell, future::Future};

#[derive(Clone, Copy, Default)]
pub struct Work {
    pub sst_visits: usize,
    pub filter_negatives: usize,
    pub planned_blocks: usize,
}

tokio::task_local! { static WORK: Cell<Work>; }

pub async fn measure<F: Future>(future: F) -> (F::Output, Work) {
    WORK.scope(Cell::default(), async {
        let result = future.await;
        (result, WORK.with(Cell::get))
    }).await
}

pub(crate) fn add(update: impl FnOnce(&mut Work)) {
    let _ = WORK.try_with(|cell| {
        let mut work = cell.get();
        update(&mut work);
        cell.set(work);
    });
}
'''
    replace(crate / 'reader.rs', '        if !work.is_empty() {',
            '        crate::blob_work::add(|counts| counts.sst_visits += work.len());\n'
            '        if !work.is_empty() {')
    replace(crate / 'multi_sst.rs', '                    stats.sst_filter_point_negatives.increment(1);',
            '                    stats.sst_filter_point_negatives.increment(1);\n'
            '                    crate::blob_work::add(|counts| counts.filter_negatives += 1);')
    replace(crate / 'multi_sst.rs', '    needed.dedup();',
            '    needed.dedup();\n'
            '    crate::blob_work::add(|counts| counts.planned_blocks += needed.len());')

    storage = native / 'src/storage.rs'
    replace(storage, 'keys, transaction_id, max_response_bytes, &mut None)',
            'keys, transaction_id, max_response_bytes, &mut None, false, &mut None)')
    replace(storage, '        database_timing: &mut Option<(Duration, Duration)>,',
            '        database_timing: &mut Option<(Duration, Duration)>,\n'
            '        measure_blob_work: bool,\n'
            '        blob_work: &mut Option<slatedb::blob_work::Work>,')
    replace(storage, '                let values = match &*database {',
            '                let read = async { match &*database {')
    replace(storage, '                };\n                *database_timing',
            '                }};\n'
            '                let values = if measure_blob_work {\n'
            '                    let (result, counts) = slatedb::blob_work::measure(read).await;\n'
            '                    *blob_work = Some(counts);\n'
            '                    result\n'
            '                } else { read.await };\n'
            '                *database_timing')
    replace(storage, '                let values = transaction.multi_get(keys).await.map_err(storage_error);',
            '                let read = transaction.multi_get(keys);\n'
            '                let values = if measure_blob_work {\n'
            '                    let (result, counts) = slatedb::blob_work::measure(read).await;\n'
            '                    *blob_work = Some(counts);\n'
            '                    result\n'
            '                } else { read.await }.map_err(storage_error);')
    service = native / 'src/service/kv.rs'
    replace(service, '        let mut database_timing = None;',
            '        let mut database_timing = None;\n'
            '        let mut blob_work = None;\n'
            '        let measure_blob_work = request.keys.iter().all(|key| key.len() == 34 && key.starts_with(b"b:"))\n'
            '            && std::env::var_os("VAULTICDB_DIAGNOSTIC_BLOB_WORK").as_deref() == Some(std::ffi::OsStr::new("1"));')
    replace(service, '                &mut database_timing,',
            '                &mut database_timing,\n'
            '                measure_blob_work,\n'
            '                &mut blob_work,')
    replace(service, '        set_database_read_timing(&mut response, database_timing);',
            '        set_database_read_timing(&mut response, database_timing);\n'
            '        if let Some(work) = blob_work {\n'
            '            for (name, value) in [\n'
            '                ("x-vaulticdb-blob-sst-visits", work.sst_visits),\n'
            '                ("x-vaulticdb-blob-filter-negatives", work.filter_negatives),\n'
            '                ("x-vaulticdb-blob-planned-blocks", work.planned_blocks),\n'
            '            ] {\n'
            '                response.metadata_mut().insert(name, value.to_string().parse().expect("decimal metadata"));\n'
            '            }\n'
            '        }')
    for path, source in pending.items():
        path.write_text(source)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('engine', type=Path)
    parser.add_argument('native', type=Path)
    args = parser.parse_args()
    install(args.engine, args.native)