import hashlib
import importlib.util
import os
import subprocess
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('rootshell', 'scripts/rootshell-manifest.py')
rootshell = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rootshell)


class RootshellReleaseTests(unittest.TestCase):
    def test_channel_promotion_never_regresses(self):
        self.assertTrue(rootshell.should_promote({'version': '0.1.9'}, {'version': '0.1.10'}))
        self.assertFalse(rootshell.should_promote({'version': '0.2.0'}, {'version': '0.1.10'}))
        self.assertFalse(rootshell.should_promote({'version': '0.1.0'}, {'version': '0.1.0'}))
        with self.assertRaises(ValueError):
            rootshell.should_promote({'version': '0.1.0', 'notes': 'old'}, {'version': '0.1.0', 'notes': 'new'})

    def test_install_and_checksum_failure(self):
        installer = Path('scripts/rootshell-install.sh').resolve()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            mocks = root / 'mocks'
            mocks.mkdir()
            curl = mocks / 'curl'
            curl.write_text('''#!/bin/sh
while [ "$1" != -o ]; do shift; done
case "$3" in
  */rootshell.json) printf '{\n  "version": "0.1.0"\n}\n' > "$2" ;;
  *.sha256) asset=${3##*/}; printf '%s  %s\\n' "$TEST_DIGEST" "${asset%.sha256}" > "$2" ;;
  *) printf test-binary > "$2" ;;
esac
''')
            curl.chmod(0o755)
            env = dict(os.environ, HOME=str(root), SHELL='/bin/bash',
                       PATH=f'{mocks}:/usr/bin:/bin',
                       TEST_DIGEST=hashlib.sha256(b'test-binary').hexdigest())
            for _ in range(2):
                subprocess.run(['sh', str(installer)], env=env, check=True, capture_output=True)
            dest = root / '.local/opt/herdr-rootshell/bin/herdr'
            self.assertEqual(dest.read_bytes(), b'test-binary')
            self.assertEqual((root / '.bashrc').read_text().count('# rootshell-herdr'), 1)
            env['TEST_DIGEST'] = '0' * 64
            result = subprocess.run(['sh', str(installer)], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(dest.read_bytes(), b'test-binary')

    def test_assets_and_tag_validation(self):
        with tempfile.TemporaryDirectory() as folder:
            assets = Path(folder)
            for target in ('linux-x86_64', 'linux-aarch64', 'macos-x86_64', 'macos-aarch64', 'windows-x86_64.zip'):
                (assets / f'herdr-{target}').write_bytes(b'binary')
            result = rootshell.manifest('rootshell-v0.1.2', 'abc', assets_dir=assets)
            self.assertEqual(result['version'], '0.1.2')
            self.assertEqual(result['assets']['windows-x86_64']['format'], 'zip')
            self.assertEqual(result['assets']['macos-aarch64']['sha256'], hashlib.sha256(b'binary').hexdigest())
            with self.assertRaises(ValueError):
                rootshell.manifest('v0.1.2', 'abc', assets_dir=assets)
