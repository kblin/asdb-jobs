# License: GNU Affero General Public License v3 or later
# A copy of GNU AGPL v3 should have been included in this software package in LICENSE.txt.

"""Core job runner logic"""

import asyncio
from asyncio import Future
from asyncio.subprocess import Process
from enum import Enum
from io import StringIO
import subprocess

from aiostandalone import StandaloneApplication

from .blast import (
    ClusterBlastResult,
    ComparippsonResult,
    parse_blast,
)
from .config import RunConfig
from .errors import (
    ASDBJobsError,
    InvalidJobData,
    InvalidJobType,
)
from .models.control import Control
from .models.job import Job, JobQueue
from . import get_version


CONTROL_UPDATE_SLEEP = 5


class JobOutcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEOUT = 'timeout'
    INTERNAL_ERROR = "internal_error"


async def dispatch(app: StandaloneApplication) -> None:
    """Dispatch a background job"""
    config: RunConfig = app['conf']
    config.up()
    queue = JobQueue(config.db, config.name)
    while True:
        config.read_config()

        if config.want_less_jobs:
            app.logger.debug("Shutting down a task")
            config.down()
            break

        job = await queue.get_next()
        if job is None:
            await asyncio.sleep(CONTROL_UPDATE_SLEEP)
            continue

        try:
            await handle_job(app, job)
        except ASDBJobsError as err:
            job.status = "failed"
            job.results ={"status": "failed", "error": str(err)}
            await job.commit()


async def handle_job(app: StandaloneApplication, job: Job):
    """Run a given job"""
    config: RunConfig = app['conf']
    app.logger.debug("Handling %s job %s", job.jobtype, job.id)
 
    if job.jobtype == "comparippson":
        await handle_comparippson(app, job)
    elif job.jobtype == "clusterblast":
        await handle_clusterblast(app, job)
    else:
        raise InvalidJobType(job.jobtype)

    app.logger.debug("Done with %s job %s", job.jobtype, job.id)

async def handle_comparippson(app: StandaloneApplication, job: Job) :
    """Run a comparippson job"""
    config: RunConfig = app["conf"]

    cmdline: list[str] = [
        "podman", "run", "--detach=false", "--rm", "--interactive",
        "--volume", f"{config.db_dir}:/databases:ro",
        "--name", job.id,
        "docker.io/antismash/asdb-jobs:latest",
        "blastp",
        "-num_threads", "4",
        "-db", "/databases/comparippson/asdb/3.9/cores.fa",
        "-outfmt", "6 qacc sacc nident qseq qstart qend qlen sseq sstart send slen",
    ]

    data = f">{job.data['name']}\n{job.data['sequence']}"

    event: Future = asyncio.Future()
    proc: Process = await asyncio.create_subprocess_exec(*cmdline,
                                                         stdin=subprocess.PIPE,
                                                         stdout=subprocess.PIPE,
                                                         stderr=subprocess.PIPE)

    def timeout_handler():
        asyncio.ensure_future(cancel(app, event, job.id))
    timeout = app.loop.call_later(3600, timeout_handler)
    task = asyncio.ensure_future(run_process(app, proc, event, data))

    res, stdout, stderr = await event
    if res == JobOutcome.TIMEOUT:
        task.cancel()
        job.status = "failed"
        job.results = {"status": "failed", "error": "timeout exceeded"}
        await job.commit()
        return
    
    timeout.cancel()

    if res == JobOutcome.FAILURE:
        job.status = "failed"
        job.results = {"status": "failed", "error": stderr}
        await job.commit()
        return

    job.status = "done"

    metadata = app['comparippson_metadata']
    results = [] 
    for blast_res in parse_blast(stdout):
        results.append(ComparippsonResult.from_blast(blast_res, metadata).to_json())

    results.sort(key=lambda e: e["identity"], reverse=True)

    job.results = {"hits": results}
    await job.commit()


async def handle_clusterblast(app: StandaloneApplication, job: Job):
    """Run a ClusterBLast job"""
    config: RunConfig = app["conf"]
    cmdline: list[str] = [
        "podman", "run", "--detach=false", "--rm", "--interactive",
        "--volume", f"{config.db_dir}:/databases:ro",
        "--name", job.id,
        "docker.io/antismash/asdb-jobs:latest",
        "diamond", "blastp",
        "--db", "/databases/clusterblast/proteins",
        "--compress", "0",
        "--max-target-seqs", "50",
        "--evalue", "1e-05",
        "--outfmt", "6", "qseqid", "sseqid", "nident", "qseq", "qstart", "qend", "qlen", "sseq", "sstart", "send", "slen",
    ]

    data = f">{job.data['name']}\n{job.data['sequence']}"

    event: Future = asyncio.Future()
    proc: Process = await asyncio.create_subprocess_exec(*cmdline,
                                                         stdin=subprocess.PIPE,
                                                         stdout=subprocess.PIPE,
                                                         stderr=subprocess.PIPE)

    def timeout_handler():
        asyncio.ensure_future(cancel(app, event, job.id))
    timeout = app.loop.call_later(3600, timeout_handler)
    task = asyncio.ensure_future(run_process(app, proc, event, data))

    res, stdout, stderr = await event
    if res == JobOutcome.TIMEOUT:
        task.cancel()
        job.status = "failed"
        job.results = {"status": "failed", "error": "timeout exceeded"}
        await job.commit()
        return
    
    timeout.cancel()

    if res == JobOutcome.FAILURE:
        job.status = "failed"
        job.results = {"status": "failed", "error": stderr}
        await job.commit()
        return

    job.status = "done"

    results = [] 
    for blast_res in parse_blast(stdout):
        results.append(ClusterBlastResult.from_blast(blast_res).to_json())

    job.results = {"hits": results}
    await job.commit()


async def cancel(app: StandaloneApplication, event: Future, container_name: str):
    """Kill the container once the timeout has expired"""

    app.logger.debug("Timeout expired, killing container %s", container_name)

    proc = await asyncio.create_subprocess_exec("podman", "kill", container_name, stdout=subprocess.DEVNULL)
    await proc.communicate()
    del app['containers'][container_name]
    event.set_result((JobOutcome.TIMEOUT, [], ["Runtime exceeded"], []))


async def run_process(app: StandaloneApplication, proc: Process, event: Future, stdin: str = None):
    """Run the actual job"""
    stdin_raw = None
    if stdin:
        stdin_raw = stdin.encode("utf-8")
    stdout_raw, stderr_raw = await proc.communicate(stdin_raw)
    stdout = stdout_raw.decode("utf-8").splitlines()
    stderr = stderr_raw.decode("utf-8").splitlines()
    if proc.returncode != 0:
        event.set_result((JobOutcome.FAILURE, stdout, stderr))
        return

    event.set_result((JobOutcome.SUCCESS, stdout, stderr))


async def manage(app: StandaloneApplication) -> None:
    """Run the main management process."""
    config: RunConfig = app['conf']
    version = await get_version()
    control = Control(
        engine=config.db,
        name=config.name,
        status="running",
        stop_scheduled=False,
        version=version,
    )
    await control.commit()
    while True:
        await control.fetch()

        if control.stop_scheduled:
            config.max_jobs = 0

        if config.want_more_jobs:
            app.logger.debug("Starting an extra task")
            app.start_task(dispatch)

        await asyncio.sleep(CONTROL_UPDATE_SLEEP)

        if config.running_jobs == 0 and not config.want_more_jobs:
            break

    await control.delete()
