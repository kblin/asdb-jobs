# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""asdb-jobs command line handling"""

import argparse

from aiostandalone import StandaloneApplication
import json
import multiprocessing
from pathlib import Path

from .config import RunConfig, init_db, close_db
from .core import dispatch, manage
from .log import core_logger, setup_logging
from . import get_version_sync

ROOT_DIR = Path(__file__).parent.parent
DEFAULT_CONFIGFILE =  ROOT_DIR / "asdb-jobs.toml"
DEFAULT_DBDIR = ROOT_DIR / "databases"
DEFAULT_JOBS = 5
DEFAULT_CPUS = max(1, multiprocessing.cpu_count() // DEFAULT_JOBS)
DEFAULT_WORKDIR = ROOT_DIR / "workdir"

def main():
    """Run the ASDB background job runner"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", type=Path, default=DEFAULT_CONFIGFILE,
                        help="Location of the configuration file to use (default: %(default)s)")
    parser.add_argument("-C", "--cpus", type=int, default=DEFAULT_CPUS,
                        help="How many CPUs to use per job (default: %(default)s)")
    parser.add_argument("-D", "--db-dir", type=Path, default=DEFAULT_DBDIR,
                        help="directory containing the database files to use (default: %(default)s)")
    parser.add_argument("-j", "--max-jobs", type=int, default=DEFAULT_JOBS,
                        help="How many background jobs to run (default: %(default)s)")
    parser.add_argument("-n", "--name", default="asdb-jobs",
                        help="Name of the job runner (default: %(default)s)")
    parser.add_argument("-V", "--version", action="version", version=get_version_sync())
    parser.add_argument("-w", "--workdir", type=Path, default=DEFAULT_WORKDIR,
                        help="working directory to keep the job files in (default: %(default)s)")
    
    args = parser.parse_args()
    setup_logging()

    app = StandaloneApplication(logger=core_logger)

    # init with the command line settings
    config = RunConfig.from_argparse(args)
    # load remaining settings from the config file
    config.read_config()
    app['conf'] = config

    comparippson_metadata_file = config.db_dir / "comparippson" / "asdb" / "3.9" / "metadata.json"
    with comparippson_metadata_file.open("r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    app['comparippson_metadata'] = metadata

    app.on_startup.append(init_db)

    app.on_cleanup.append(close_db)

    for _ in range(config.max_jobs):
        app.tasks.append(dispatch)

    app.main_task = manage

    app.run()

if __name__ == "__main__":
    main()