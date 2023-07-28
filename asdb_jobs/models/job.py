# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Database models for background jobs"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from aiopg.sa import Engine, SAConnection
from aiopg.sa.result import RowProxy
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from asdb_jobs.errors import InvalidJobId, JobConfict

metadata = sa.MetaData(schema="asdb_jobs")

JOBS = sa.Table("jobs", metadata,
                sa.Column("id", sa.Text, primary_key=True),
                sa.Column("jobtype", sa.Text),
                sa.Column("status", sa.Text),
                sa.Column("runner", sa.Text),
                sa.Column("submitted_date", sa.Date),
                sa.Column("data", JSONB),
                sa.Column("results", JSONB),
                sa.Column("version", sa.Integer),
               )


async def get_job_by_id(conn: SAConnection, id: str) -> RowProxy:
    """Get the controls row matching """
    row = await (
        await conn.execute(JOBS.select().where(JOBS.c.id == id))    
        ).first()
    if row is None:
        raise InvalidJobId(f"Job({id}) not found in database")
    return row


@dataclass
class Job:
    """A background job"""
    engine: Engine
    id: str
    jobtype: str
    status: str
    runner: str
    data: Dict[str, Any] = field(default_factory=dict)
    results: Dict[str, Any] = field(default_factory=dict)
    version: int = field(default=0, hash=True)

    @classmethod
    async def from_db(cls, engine: Engine, id: str) -> "Job":
        """Load job from database"""
        async with engine.acquire() as conn:
            row = await get_job_by_id(conn, id)

        return cls(engine, row["id"], row["jobtype"], row["status"], row["runner"], row["data"], row["results"], row["version"])
    
    async def fetch(self) -> "Job":
        """Fetch the current job state """
        async with self.engine.acquire() as conn:
            row = await get_job_by_id(conn, self.id)
        self.status = row["status"]
        self.runner = row["runner"]
        self.data = row["data"]
        self.results = row["results"]
        self.version = row["version"]

        return self
    
    async def commit(self) -> "Job":
        """Commit the current job status to the database"""
        async with self.engine.acquire() as conn:
            try:
                row = await get_job_by_id(conn, self.id)
                if row["version"] != self.version:
                    raise JobConfict(f"Job({self.id}) changed in database {row['version']} vs. local {self.version}")
                self.version += 1
                await conn.execute(sa.update(JOBS).values({
                    "status": self.status,
                    "runner": self.runner,
                    "data": self.data,
                    "results": self.results,
                    "version": self.version,
                }).where(JOBS.c.id == self.id, JOBS.c.version == row["version"]))
            except InvalidJobId:
                await conn.execute(sa.insert(JOBS).values({
                    "id": self.id,
                    "jobtype": self.id,
                    "status": self.status,
                    "runner": self.runner,
                    "data": self.data,
                    "results": self.results,
                    "version": self.version,
                }))

        return self
    

@dataclass
class JobQueue:
    engine: Engine
    name: str
    
    async def get_next(self) -> Optional[Job]:
        """Get the next available job"""
        async with self.engine.acquire() as conn:
            async with conn.begin() as tx:
                row = await (
                    await conn.execute(JOBS.select().where(JOBS.c.status == "pending").with_for_update(skip_locked=True).limit(1))
                    ).first()
                if row is None:
                    return None

                await conn.execute(sa.update(JOBS).values({
                    "runner": self.name,
                    "status": "running",
                    "version": row["version"] + 1
                }).where(JOBS.c.id == row["id"], JOBS.c.version == row["version"]))
        return await Job.from_db(self.engine, row["id"])