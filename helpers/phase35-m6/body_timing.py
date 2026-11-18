import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tomllib


BEFORE = '''                maybe_spawn_blocking(move || {
                    use crate::local::read_range;

                    let buffer = read_range(&mut file, &path, self.range)?;

                    Ok(buffer)
                })
                .await'''
AFTER = '''                let observer = self.extensions.get::<vaultic_body_timing::FileReadObservation>().cloned();
                let worker = observer.map(|observer| observer.submit(tokio::runtime::Handle::try_current().is_ok()));
                let waiter = worker.as_ref().map(|worker| worker.waiter());
                let result = maybe_spawn_blocking(move || {
                    use crate::local::read_range_observed;

                    let worker = worker.map(|worker| worker.start());
                    let range = worker.as_ref().map(|worker| worker.range_read(self.range.end - self.range.start));
                    let buffer = read_range_observed(&mut file, &path, self.range, range);
                    if let Some(worker) = worker {
                        worker.finish(buffer.is_ok());
                    }
                    buffer
                })
                .await;
                if let Some(waiter) = waiter {
                    waiter.resume();
                }
                result'''


def patch_reader(original):
    signature = '''pub(crate) fn read_range(
    file: &mut File,
    path: &std::path::Path,
    range: Range<u64>,
) -> Result<Bytes> {'''
    if original.count(signature) != 1:
        raise ValueError('pinned range-reader signature differs')
    start = original.index(signature)
    end = original.index('\n}\n', start) + len('\n}\n')
    reader = original[start:end]
    replacements = (
        (signature, signature + '''
    read_range_observed(file, path, range, None)
}

pub(crate) fn read_range_observed(
    file: &mut File,
    path: &std::path::Path,
    range: Range<u64>,
    observation: Option<vaultic_body_timing::RangeReadObservation>,
) -> Result<Bytes> {
    use vaultic_body_timing::RangeReadPhase;'''),
        ('    let mut buf = Vec::with_capacity(requested as usize);', '''    let allocation = observation.as_ref().map(|observation| observation.phase(RangeReadPhase::Allocation));
    let mut buf = Vec::with_capacity(requested as usize);
    drop(allocation);'''),
        ('        buf.resize(requested as usize, 0_u8);', '''        let zero_fill = observation.as_ref().map(|observation| observation.phase(RangeReadPhase::ZeroFill));
        buf.resize(requested as usize, 0_u8);
        drop(zero_fill);'''),
        ('            #[cfg(target_family = "unix")]\n            let read_result', '''            let read = observation.as_ref().map(|observation| observation.phase(RangeReadPhase::Read));
            #[cfg(target_family = "unix")]
            let read_result'''),
        ('            match read_result {', '''            drop(read);
            if let Some(observation) = observation.as_ref() {
                observation.read_result(&read_result);
            }
            match read_result {'''),
        ('    #[cfg(all(not(windows), not(unix)))]\n    {', '''    #[cfg(all(not(windows), not(unix)))]
    {
        if let Some(observation) = observation.as_ref() {
            observation.unsupported();
        }'''),
        ('    Ok(buf.into())', '''    let conversion = observation.as_ref().map(|observation| observation.phase(RangeReadPhase::Conversion));
    let buffer = buf.into();
    drop(conversion);
    if let Some(observation) = observation {
        observation.finish();
    }
    Ok(buffer)'''),
    )
    for before, after in replacements:
        if reader.count(before) != 1:
            raise ValueError('pinned range-reader stage differs: ' + before)
        reader = reader.replace(before, after, 1)
    return original[:start] + reader + original[end:]


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def prepare(workspace, output):
    if output.parent.resolve() != Path('/run') or not output.name.startswith('vaultic-body-hook-') or output.exists():
        raise ValueError('diagnostic output must be a new direct /run/vaultic-body-hook-* directory')
    lock = tomllib.loads((workspace / 'vaulticdb/Cargo.lock').read_text())
    packages = [package for package in lock['package'] if package['name'] == 'object_store' and package['version'] == '0.14.1']
    if len(packages) != 1 or packages[0]['source'] != 'registry+https://github.com/rust-lang/crates.io-index':
        raise ValueError('expected locked registry object_store 0.14.1')
    archives = list((Path.home() / '.cargo/registry/cache').glob('*/object_store-0.14.1.crate'))
    if len(archives) != 1 or digest(archives[0]) != packages[0]['checksum']:
        raise ValueError('locked object-store crate archive checksum differs')
    modified = subprocess.check_output(['git', '-C', str(workspace), 'diff', '--name-only', 'HEAD'], text=True).splitlines()
    new_files = ['helpers/phase35-m6/body_timing.py', 'vaulticdb/diagnostics/body-timing/Cargo.toml',
                 'vaulticdb/diagnostics/body-timing/src/lib.rs']
    pins = {}
    for name in sorted(set(modified + new_files)):
        path = workspace / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(workspace):
            raise ValueError('overlay is not a workspace-owned regular file: ' + name)
        pins[name] = digest(path)
    output.mkdir(mode=0o700)
    output.chmod(0o700)
    candidate = output / 'candidate'
    candidate.mkdir(mode=0o700)
    archive = subprocess.Popen(['git', '-C', str(workspace), 'archive', 'HEAD'], stdout=subprocess.PIPE)
    try:
        subprocess.run(['tar', '-x', '-C', str(candidate)], stdin=archive.stdout, check=True)
    finally:
        archive.stdout.close()
    if archive.wait():
        raise ValueError('isolated source archive failed')
    for name, pin in pins.items():
        destination = candidate / name
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        destination.write_bytes((workspace / name).read_bytes())
        destination.chmod(0o600)
        if digest(destination) != pin:
            raise ValueError('isolated overlay pin differs: ' + name)
    with (output / 'workspace.patch').open('xb') as patch:
        subprocess.run(['git', '-C', str(workspace), 'diff', '--binary', 'HEAD'], stdout=patch, check=True)
    with tarfile.open(archives[0]) as archive:
        archive.extractall(output, filter='data')
    dependency = output / 'object_store-0.14.1'
    dependency.chmod(0o700)
    library = dependency / 'src/lib.rs'
    original = library.read_text()
    if original.count(BEFORE) != 1:
        raise ValueError('pinned file-body implementation anchor differs')
    patched = original.replace(BEFORE, AFTER, 1)
    library.write_text(patched)
    library.chmod(0o600)
    local = dependency / 'src/local.rs'
    original_local = local.read_text()
    patched_local = patch_reader(original_local)
    local.write_text(patched_local)
    local.chmod(0o600)
    manifest = dependency / 'Cargo.toml'
    original_manifest = manifest.read_text()
    if 'vaultic-body-timing' in original_manifest:
        raise ValueError('registry manifest unexpectedly already contains the diagnostic hook')
    patched_manifest = original_manifest + '\n[dependencies.vaultic-body-timing]\npath = ' + json.dumps(str(candidate / 'vaulticdb/diagnostics/body-timing')) + '\n'
    manifest.write_text(patched_manifest)
    manifest.chmod(0o600)
    (output / 'dependency.patch').write_text(''.join(difflib.unified_diff(original.splitlines(True), patched.splitlines(True), 'a/src/lib.rs', 'b/src/lib.rs')) +
                                           ''.join(difflib.unified_diff(original_local.splitlines(True), patched_local.splitlines(True), 'a/src/local.rs', 'b/src/local.rs')) +
                                           ''.join(difflib.unified_diff(original_manifest.splitlines(True), patched_manifest.splitlines(True), 'a/Cargo.toml', 'b/Cargo.toml')))
    (output / 'cargo-config.toml').write_text('[patch.crates-io]\nobject_store = { path = ' + json.dumps(str(dependency)) + ' }\n')
    captured = {
        'head': subprocess.check_output(['git', '-C', str(workspace), 'rev-parse', 'HEAD'], text=True).strip(),
        'diagnostic_only': True, 'registry_modified': False, 'dependency_archive_sha256': digest(archives[0]),
        'overlay_pins': pins, 'candidate': str(candidate), 'dependency': str(dependency),
        'feature': 'diagnostic-body-timing',
        'artifacts': {str(path.relative_to(output)): digest(path) for path in
                      (library, local, manifest, output / 'dependency.patch', output / 'cargo-config.toml', output / 'workspace.patch')},
    }
    (output / 'preparation.json').write_text(json.dumps(captured, indent=2) + '\n')
    for path in output.iterdir():
        if path.is_file():
            path.chmod(0o600)
    print(json.dumps(captured, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    prepare(Path(__file__).resolve().parents[2], args.output)