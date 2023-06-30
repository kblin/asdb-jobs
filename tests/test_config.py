from argparse import Namespace
from pathlib import Path

import pytest

from asdb_jobs import config


def test_from_argparse():
    args = Namespace(
        configfile=Path(__file__).parent / "test.toml",
        cpus=1,
        debug=True,
        name="test-name",
        max_jobs=3,
    )

    conf = config.RunConfig.from_argparse(args)
    assert conf.configfile == args.configfile
    assert conf.cpus == args.cpus
    assert conf.debug == args.debug
    assert conf.name == args.name
    assert conf.max_jobs == args.max_jobs


def test_from_argparse_invalid():
    args = Namespace()

    with pytest.raises(ValueError):
        config.RunConfig.from_argparse(args)


def test_update_from_dict():
    args = Namespace(
        configfile=Path(__file__).parent / "test.toml",
        cpus=1,
    )
    updates = {"cpus": 2}
    conf = config.RunConfig.from_argparse(args)
    assert conf.cpus == 1
    conf.update_from_dict(updates)
    assert conf.cpus == 2
