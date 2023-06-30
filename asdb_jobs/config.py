# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Configuration helpers"""

from argparse import Namespace
from dataclasses import (
    dataclass,
    field,
    fields,
    _MISSING_TYPE,
)
from hashlib import md5
from pathlib import Path
from typing import Any, Optional

from aiopg.sa import create_engine, Engine
from aiostandalone import StandaloneApplication

try:
    import tomllib as toml
except ImportError:
    import tomlkit as toml

_ROOT_DIR = Path(__file__).parent.parent


@dataclass
class RunConfig:
    """Runtime config"""
    configfile: Path

    cpus: int = 2
    db_dir: Path = _ROOT_DIR / "databases"
    debug: bool = False,
    max_jobs: int = 5
    name: str = "asdb-jobs"
    workdir: Path = _ROOT_DIR / "workdir"

    # database settings
    host: str = "localhost"
    database: str = "antismash"
    password: str = "secret"
    port: int = 5432
    user: str = "postgres"

    _config_file_hash: str = field(init=False, default="")
    _db: Optional[Engine] = field(init=False, default=None)
    _running_jobs: int = field(init=False, default=0)

    def __post_init__(self):
        """"""

    def read_config(self, force: bool=False) -> None:
        """Read the config file"""
        with self.configfile.open("r", encoding="utf-8") as handle:
            data = handle.read()
            digest = md5(data.encode("utf-8")).hexdigest()
            if digest == self._config_file_hash and not force:
                # Config file didn't change, just return
                return
            self._config_file_hash = digest
            loaded = toml.loads(data)
        self.update_from_dict(loaded)

    def update_from_dict(self, config: dict[str, Any]) -> None:
        """Update the running config from a dict, like loaded from a config file"""

        for arg in fields(RunConfig):
            if arg.name in config:
                setattr(self, arg.name, config[arg.name])

    async def init_db(self) -> Engine:
        """Initialise the database connection"""
        self._db = await create_engine(
            user=self.user,
            database=self.database,
            host=self.host,
            port=self.port,
            password=self.password,
        )
        return self._db

    @property
    def db(self) -> Engine:
        """Get the database engine"""
        if self._db is None:
            raise ValueError("Initialise the database first")
        return self._db

    async def close_db(self) -> None:
        """Close all db connections"""
        if self._db is None:
            return
        self._db.close()
        await self._db.wait_closed()
        self._db = None

    @property
    def running_jobs(self) -> int:
        """Get the number of currently running jobs"""
        return self._running_jobs
    
    def up(self):
        """Called when a dispatcher starts up"""
        self._running_jobs += 1

    def down(self):
        """Called when a dispatcher exits"""
        self._running_jobs -= 1

    @property
    def want_more_jobs(self) -> bool:
        """Check if we want to run more background jobs"""
        return self.running_jobs < self.max_jobs
    
    @property
    def want_less_jobs(self) -> bool:
        """Check if we want to run less background jobs"""
        return self.running_jobs > self.max_jobs

    @classmethod
    def from_argparse(cls, args: Namespace) -> "RunConfig":
        """Instantiate from an argparse.Namespace"""
        kwargs: dict[str, any] = {}
        for arg in fields(RunConfig):
            if not arg.init:
                continue
            if arg.name in args:
                kwargs[arg.name] = getattr(args, arg.name)
            else:
                if not isinstance(arg.default, _MISSING_TYPE):
                    kwargs[arg.name] = arg.default
                elif not isinstance(arg.default_factory, _MISSING_TYPE):
                    kwargs[arg.name] = arg.default_factory()
                else:
                    raise ValueError(f"Missing value for {arg.name}")

        return cls(**kwargs)


async def init_db(app: StandaloneApplication):
    """Initialise the database connection"""
    conf: RunConfig = app['conf']
    app.logger.debug("Initialising the database connection")

    engine= await conf.init_db()
    app['engine'] = engine


async def close_db(app: StandaloneApplication):
    """Close the database connection"""
    conf: RunConfig = app['conf']
    app.logger.debug("Closing the database connection")
    await conf.close_db()