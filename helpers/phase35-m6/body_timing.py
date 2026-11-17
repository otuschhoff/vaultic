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
                    use crate::local::read_range;

                    let worker = worker.map(|worker| worker.start());
                    let buffer = read_range(&mut file, &path, self.range);
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
    manifest = dependency / 'Cargo.toml'
    original_manifest = manifest.read_text()
    if 'vaultic-body-timing' in original_manifest:
        raise ValueError('registry manifest unexpectedly already contains the diagnostic hook')
    patched_manifest = original_manifest + '\n[dependencies.vaultic-body-timing]\npath = ' + json.dumps(str(candidate / 'vaulticdb/diagnostics/body-timing')) + '\n'
    manifest.write_text(patched_manifest)
    manifest.chmod(0o600)
    (output / 'dependency.patch').write_text(''.join(difflib.unified_diff(original.splitlines(True), patched.splitlines(True), 'a/src/lib.rs', 'b/src/lib.rs')) +
                                           ''.join(difflib.unified_diff(original_manifest.splitlines(True), patched_manifest.splitlines(True), 'a/Cargo.toml', 'b/Cargo.toml')))
    (output / 'cargo-config.toml').write_text('[patch.crates-io]\nobject_store = { path = ' + json.dumps(str(dependency)) + ' }\n')
    captured = {
        'head': subprocess.check_output(['git', '-C', str(workspace), 'rev-parse', 'HEAD'], text=True).strip(),
        'diagnostic_only': True, 'registry_modified': False, 'dependency_archive_sha256': digest(archives[0]),
        'overlay_pins': pins, 'candidate': str(candidate), 'dependency': str(dependency),
        'feature': 'diagnostic-body-timing',
        'artifacts': {str(path.relative_to(output)): digest(path) for path in
                      (library, manifest, output / 'dependency.patch', output / 'cargo-config.toml', output / 'workspace.patch')},
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