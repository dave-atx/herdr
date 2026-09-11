"""Generate the fork updater contract from the built release assets."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def manifest(tag, commit, root=Path('.'), assets_dir=Path('release-assets')):
    match = re.fullmatch(r'rootshell-v(\d+\.\d+\.\d+)', tag)
    if not match:
        raise ValueError('Expected rootshell-vMAJOR.MINOR.PATCH')
    assets = {}
    for target in ('linux-x86_64', 'linux-aarch64', 'macos-x86_64', 'macos-aarch64', 'windows-x86_64'):
        name = f'herdr-{target}' + ('.zip' if target.startswith('windows') else '')
        assets[target] = {
            'url': f'https://github.com/kitknox/herdr/releases/download/{tag}/{name}',
            'sha256': hashlib.sha256((assets_dir / name).read_bytes()).hexdigest(),
        }
        if target.startswith('windows'):
            assets[target]['format'] = 'zip'
    protocol = int(re.search(r'pub const PROTOCOL_VERSION: u32 = (\d+);', (root / 'src/protocol/wire.rs').read_text())[1])
    # The endpoint generation remains the upstream compatibility contract.
    generation = int(re.search(r'pub const ENDPOINT_PROTOCOL_GENERATION: u32 = (\d+);', (root / 'src/protocol/endpoint.rs').read_text())[1])
    base_version = re.search(r'^version = "([^"]+)"', (root / 'Cargo.toml').read_text(), re.MULTILINE)[1]
    return dict(version=match[1], base_version=base_version, protocol=protocol, endpoint_generation=generation,
                notes=f'Rootshell {tag}\n\nBuilt from {commit}.', assets=assets)


def should_promote(current, candidate):
    def version(value):
        if not re.fullmatch(r'\d+\.\d+\.\d+', value):
            raise ValueError(f'Invalid fork version: {value}')
        return tuple(map(int, value.split('.')))
    old, new = version(current['version']), version(candidate['version'])
    if old == new and current != candidate:
        raise ValueError('Cannot replace an already published version with different contents')
    return new > old


def promote():
    repo = 'kitknox/herdr'
    def gh(*args):
        return subprocess.check_output(['gh', *args, '--repo', repo], text=True)
    # Read failures must abort promotion, rather than looking like an empty channel.
    tags = subprocess.check_output(['gh', 'api', f'repos/{repo}/releases', '--paginate', '--jq', '.[].tag_name'], text=True).splitlines()
    candidate = json.loads(Path('release-assets/rootshell.json').read_text())
    if 'rootshell-channel' in tags:
        release = json.loads(gh('release', 'view', 'rootshell-channel', '--json', 'assets'))
        if any(asset['name'] == 'rootshell.json' for asset in release['assets']):
            with tempfile.TemporaryDirectory() as folder:
                gh('release', 'download', 'rootshell-channel', '--pattern', 'rootshell.json', '--dir', folder)
                current = json.loads((Path(folder) / 'rootshell.json').read_text())
            if not should_promote(current, candidate):
                print('Channel already at this version or newer; leaving it unchanged.')
                return
    else:
        gh('release', 'create', 'rootshell-channel', '--target', os.environ['GITHUB_SHA'], '--prerelease', '--title', 'Rootshell update channel', '--notes', 'Mutable update manifest for Rootshell builds.')
    gh('release', 'upload', 'rootshell-channel', 'release-assets/rootshell.json', 'release-assets/install.sh', '--clobber')


if __name__ == '__main__' and '--promote' in sys.argv:
    promote()
elif __name__ == '__main__':
    result = manifest(os.environ['GITHUB_REF_NAME'], os.environ['GITHUB_SHA'])
    Path('release-assets/rootshell.json').write_text(json.dumps(result, indent=2) + '\n')
