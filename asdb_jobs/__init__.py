# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Run long-running background jobs for the antiSMASH database."""

import asyncio
from pathlib import Path
import subprocess

__version__ = "0.1.0"

_GIT_VERSION = None


async def get_version() -> str:
    """Get the full version string."""
    version = __version__

    git_ver = await get_git_version()
    if git_ver:
        version = "{}-{}".format(version, git_ver)

    return version


def get_version_sync() -> str:
    """Get the full version string, synchronous version."""
    version = __version__

    git_ver = get_git_version_sync()
    if git_ver:
        version = "{}-{}".format(version, git_ver)

    return version


async def get_git_version() -> str:
    """Get the git version."""
    global _GIT_VERSION
    if _GIT_VERSION is None:
        proc = await asyncio.create_subprocess_exec('git', 'rev-parse', '--short', 'HEAD',
                                                    cwd=Path(__file__).parent)

        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            ver = ""
        else:
            ver = stdout.strip().decode('utf-8')
        _GIT_VERSION = ver

    return _GIT_VERSION


def get_git_version_sync() -> str:
    """Get the git version, synchronous version."""
    global _GIT_VERSION
    if _GIT_VERSION is None:
        proc = subprocess.Popen(['git', 'rev-parse', '--short', 'HEAD'],
                                cwd=Path(__file__).parent,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            ver = ""
        else:
            ver = stdout.strip().decode('utf-8')
        _GIT_VERSION = ver

    return _GIT_VERSION