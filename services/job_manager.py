"""
services/job_manager.py
=======================
Background job management.

Runs collection generators in a daemon thread so Streamlit's main thread
remains unblocked. The UI polls get_job() to read progress and final results.

Architecture notes
------------------
Shared state is held in class-level dicts (_jobs, _threads,
_cancellation_events). These are shared across all callers by virtue of being
class attributes — no singleton enforcement is required or present.

Hash lifecycle
--------------
Two hashes exist per job and they are intentionally different:

  config_hash (in ScrapeRun.config_hash)
    Generated BEFORE params.job_id is set.
    Represents the pure parameter fingerprint.

  collection_hash (in CollectionResult, ReplicabilityLog)
    Generated locally AFTER params.job_id is set.
    Includes the UUID job_id so every run produces a unique hash.

Cancellation model
------------------
Cancellation is COOPERATIVE, not preemptive. The cancel signal is checked at
the top of each generator iteration.
"""

import logging
import json
import hashlib
import threading
import time
import uuid
from typing import Any, Dict

from core.schemas import CollectionParams, JobState, JobStatus
from utils.db_helpers import create_scrape_run, log_action, update_scrape_run

logger = logging.getLogger(__name__)


def generate_collection_hash(params_dict):
    """Utility for hashing collection parameters."""
    param_str = json.dumps(params_dict, sort_keys=True)
    return hashlib.sha256(param_str.encode()).hexdigest()[:16]


class JobManager:
    """
    Manages background jobs.

    State is held in class-level dicts — all classmethods operate on shared
    state without requiring instantiation.
    """imit.
    """

    # Class-level shared state — these ARE the singleton mechanism
    _jobs:                Dict[str, JobState]          = {}
    _threads:             Dict[str, threading.Thread]  = {}
    _cancellation_events: Dict[str, threading.Event]   = {}
    
    # 1. Add class-level thread lock for job creation synchronization
    _job_lock = threading.Lock()

    @classmethod
    def start_job(cls, data_service: Any, params: CollectionParams) -> str:
        """
        Start a background collection job.

        Spawns a daemon thread running data_service.collect_data(params).
        The thread writes progress into cls._jobs[job_id] which the UI polls
        via get_job().

        params.job_id is set inside the worker (not before) to ensure
        config_hash is computed without it — see module docstring.

        params.session_id must be set by the caller before calling start_job().

        Returns the job_id string. The caller stores this in
        st.session_state.scraping_job_id for the polling loop.
        """
        # 2. Acquire lock before checking active count or creating job
        with cls._job_lock:
            if cls.active_job_count() > 0:
                raise RuntimeError("Another processing job is already running.")
            
            job_id = str(uuid.uuid4())

            cls._jobs[job_id] = JobState(
                job_id     = job_id,
                status     = JobStatus.PENDING,
                started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )

            cancel_event = threading.Event()
            cls._cancellation_events[job_id] = cancel_event

            def worker():
                cls._jobs[job_id].status = JobStatus.RUNNING

                # Compute config_hash BEFORE setting params.job_id so the hash
                # reflects only the collection parameters, not the run UUID.
                # See module docstring — changing this order breaks hash invariants.
                config_hash = generate_collection_hash(params.model_dump())
                params.job_id = job_id

                # Attempt to create the ScrapeRun DB record.
                # On failure: log the error and continue with collection — data
                # will still be saved to collected_data via incremental saves.
                # Set params.job_id = None so downstream update_scrape_run calls
                # are skipped (no row to update) rather than raising silently.
                try:
                    create_scrape_run(
                        job_id      = job_id,
                        config_hash = config_hash,
                        parameters  = params.model_dump(),
                        session_id  = params.session_id,
                        label       = params.session_label,
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to create ScrapeRun for job %s: %s — "
                        "collection will proceed but job tracking is degraded.",
                        job_id, e,
                    )
                    log_action(
                        action     = 'job_init_failed',
                        session_id = params.session_id,
                        details    = {'job_id': job_id, 'error': str(e)},
                    )
                    # Disable downstream update_scrape_run calls — no row exists
                    params.job_id = None

                generator = data_service.collect_data(params)

                try:
                    for item in generator:
                        # Cancellation is cooperative — checked at each iteration.
                        # If the service is mid-request, cancel takes effect on the
                        # next iteration (worst case: one full request cycle).
                        if cancel_event.is_set():
                            cls._jobs[job_id].status       = JobStatus.CANCELLED
                            cls._jobs[job_id].completed_at = time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                            )
                            if params.job_id:  # may be None if create_scrape_run failed
                                update_scrape_run(job_id=job_id, status='CANCELLED')
                            log_action(
                                action     = 'job_cancelled',
                                session_id = params.session_id,
                                details    = {'job_id': job_id},
                            )
                            logger.info("Job %s cancelled.", job_id)
                            return

                        # Dispatch by duck-typing — job_manager does not import
                        # CollectionProgress or CollectionResult directly to avoid
                        # coupling. hasattr checks are the agreed protocol.
                        if hasattr(item, 'progress_percentage'):
                            # CollectionProgress — update UI-visible progress
                            cls._jobs[job_id].progress = item

                        elif hasattr(item, 'collection_hash'):
                            # CollectionResult — final yield from the generator
                            # NOTE: item.collected_posts is always [] (buffer was
                            # cleared during incremental saves). Use
                            # item.stats.total_collected for the item count.
                            cls._jobs[job_id].result       = item
                            cls._jobs[job_id].status       = JobStatus.COMPLETED
                            cls._jobs[job_id].completed_at = time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                            )
                            if params.job_id:
                                update_scrape_run(
                                    job_id          = job_id,
                                    status          = 'COMPLETED',
                                    items_collected = item.stats.total_collected,
                                )
                            log_action(
                                action     = 'job_completed',
                                session_id = params.session_id,
                                details    = {
                                    'job_id':          job_id,
                                    'items_collected': item.stats.total_collected,
                                    'collection_hash': item.collection_hash,
                                    'data_source':     getattr(params, 'data_source', 'unknown'),
                                },
                            )
                            logger.info(
                                "Job %s completed — %d items collected.",
                                job_id, item.stats.total_collected,
                            )

                        else:
                            # Unknown yield type — log and skip rather than silently ignore
                            logger.warning(
                                "Job %s: generator yielded unknown type %s — skipped.",
                                job_id, type(item).__name__,
                            )

                except Exception as e:
                    cls._jobs[job_id].status       = JobStatus.FAILED
                    cls._jobs[job_id].error        = str(e)
                    cls._jobs[job_id].completed_at = time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                    )
                    if params.job_id:
                        update_scrape_run(
                            job_id        = job_id,
                            status        = 'FAILED',
                            error_message = str(e),
                        )
                    log_action(
                        action     = 'job_crash',
                        session_id = params.session_id,
                        details    = {'job_id': job_id, 'error': str(e)},
                    )
                    logger.exception("Job %s crashed: %s", job_id, e)

            thread = threading.Thread(
                target = worker,
                daemon = True,        # dies with the process — no cleanup needed
                name   = f"CollectionJob-{job_id}",
            )
            cls._threads[job_id] = thread
            thread.start()
            logger.info("Job %s started (thread: %s).", job_id, thread.name)

            return job_id

    @classmethod
    def get_job(cls, job_id: str) -> JobState | None:
        """
        Return the current JobState for a job, or None if not found.

        Called by the polling loop on every Streamlit rerun.
        Returns None if the job was cleared or if a Streamlit hot-reload
        wiped the class-level _jobs dict — the UI handles this gracefully.
        """
        return cls._jobs.get(job_id)

    @classmethod
    def cancel_job(cls, job_id: str) -> None:
        """
        Signal a running job to cancel at its next generator iteration.

        No-op if the job is not found or is already in a terminal state
        (COMPLETED, FAILED, CANCELLED). Cancellation is cooperative — see
        module docstring for latency implications.
        """
        job = cls._jobs.get(job_id)
        if job is None:
            logger.debug("cancel_job: job %s not found — no-op.", job_id)
            return

        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            logger.debug(
                "cancel_job: job %s already in terminal state %s — no-op.",
                job_id, job.status,
            )
            return

        if job_id in cls._cancellation_events:
            cls._cancellation_events[job_id].set()
            logger.info("Job %s cancel signal sent.", job_id)

    @classmethod
    def clear_job(cls, job_id: str) -> None:
        """
        Remove all state for a completed, failed, or cancelled job.

        Called after the UI has displayed the final result.
        """
        if job_id in cls._jobs:
            del cls._jobs[job_id]
        if job_id in cls._threads:
            del cls._threads[job_id]
        if job_id in cls._cancellation_events:
            del cls._cancellation_events[job_id]
        logger.debug("Job %s cleared from memory.", job_id)

    @classmethod
    def active_job_count(cls) -> int:
        """
        Return the number of jobs currently in PENDING or RUNNING state.

        Useful for debugging and for asserting single-job invariants in tests.
        """
        return sum(
            1 for j in cls._jobs.values()
            if j.status in (JobStatus.PENDING, JobStatus.RUNNING)
        )
