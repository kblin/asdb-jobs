# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Database models for background job runner controls"""

from dataclasses import dataclass

from aiopg.sa import Engine, SAConnection
from aiopg.sa.result import RowProxy
import sqlalchemy as sa

from asdb_jobs.errors import InvalidControlName

metadata = sa.MetaData(schema="asdb_jobs")

CONTROLS = sa.Table("controls", metadata,
                    sa.Column("name", sa.Text, primary_key=True),
                    sa.Column("status", sa.Text),
                    sa.Column("stop_scheduled", sa.Boolean),
                    sa.Column("version", sa.Text),
                   )



async def get_control(conn: SAConnection, name: str) -> RowProxy:
    """Get the controls row matching """
    row = await (
        await conn.execute(CONTROLS.select().where(CONTROLS.c.name == name))    
        ).first()
    
    if row is None:
        raise InvalidControlName(f"No control named {name} found")

    return row


@dataclass
class Control:
    """Background job runner controls"""
    engine: Engine
    name: str
    status: str
    stop_scheduled: bool
    version: str

    @classmethod
    async def from_db(cls, engine: Engine, name: str) -> "Control":
        """Load relevant data from database to init object"""
        async with engine.acquire() as conn:
            row = await get_control(conn, name)
        return cls(engine, row["name"], row["status"], row["stop_scheduled"], row["version"])

    async def fetch(self) -> "Control":
        """Fetch the current control state from the database"""

        async with self.engine.acquire() as conn:
            row = await get_control(conn, self.name)

        self.status = row["status"]
        self.stop_scheduled = row["stop_scheduled"]

        return self

    async def commit(self) -> "Control":
        """Commit the current control state to the database"""
        async with self.engine.acquire() as conn:
            try:
                row = await get_control(conn, self.name)
                await conn.execute(sa.update(CONTROLS).values({
                    "name": self.name,
                    "status": self.status,
                    "stop_scheduled": self.stop_scheduled,
                    "version": self.version,
                }).where(CONTROLS.c.name == self.name))
            except InvalidControlName:
                await conn.execute(sa.insert(CONTROLS).values({
                    "name": self.name,
                    "status": self.status,
                    "stop_scheduled": self.stop_scheduled,
                    "version": self.version,
                }))
        
        return self

    async def delete(self):
        """Delete this control state from the database"""
        async with self.engine.acquire() as conn:
            row = await get_control(conn, self.name)

            await conn.execute(sa.delete(CONTROLS).where(CONTROLS.c.name == self.name))

