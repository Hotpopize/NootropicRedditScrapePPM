import uuid
import threading
from typing import Dict, Any
import time

from core.schemas import JobState, JobStatus, CollectionParams
from services.reddit_service import RedditService, generate_collection_hash
from utils.db_helpers import create_scrape_run, update_scrape_run, log_action


class JobManager:
    """
    Singleton-style manager to handle background Reddit scraping jobs.
    This prevents the Streamlit UI from blocking during long-running data collection.
    """
    _instance = None
    _jobs: Dict[str, JobState] = {}
    _threads: Dict[str, threading.Thread] = {}
    _cancellation_events: Dict[str, threading.Event] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def start_job(cls, reddit_service: Any, params: CollectionParams) -> str:
        """
        Spins up a background thread to execute the RedditService.collect_data generator.
        """
        job_id = str(uuid.uuid4())
        
        # Initialize job state
        cls._jobs[job_id] = JobState(
            job_id=job_id,
            status=JobStatus.PENDING,
            started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        
        cancel_event = threading.Event()
        cls._cancellation_events[job_id] = cancel_event
        
        # Define the background worker
        def worker():
            cls._jobs[job_id].status = JobStatus.RUNNING
            
            # Setup DB ScrapeRun state
            config_hash = generate_collection_hash(params.model_dump())
            params.job_id = job_id # Pass down so RedditService can incremental update
            try:
                create_scrape_run(job_id=job_id, config_hash=config_hash, parameters=params.model_dump(), session_id=params.session_id)
            except Exception as e:
                # If we fail to create the run, log it but maybe try to continue
                log_action(action='job_init_failed', session_id=params.session_id, details={'job_id': job_id, 'error': str(e)})

            generator = reddit_service.collect_data(params)
            
            try:
                for item in generator:
                    if cancel_event.is_set():
                        cls._jobs[job_id].status = JobStatus.CANCELLED
                        cls._jobs[job_id].completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        update_scrape_run(job_id=job_id, status='CANCELLED')
                        log_action(action='job_cancelled', session_id=params.session_id, details={'job_id': job_id})
                        return
                    
                    # Update progress or final result based on what the generator yields
                    if hasattr(item, 'progress_percentage'): # It's a CollectionProgress
                        cls._jobs[job_id].progress = item
                    elif hasattr(item, 'collection_hash'): # It's the final CollectionResult
                        cls._jobs[job_id].result = item
                        cls._jobs[job_id].status = JobStatus.COMPLETED
                        cls._jobs[job_id].completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        update_scrape_run(job_id=job_id, status='COMPLETED', items_collected=item.stats.total_collected)
                        
            except Exception as e:
                cls._jobs[job_id].status = JobStatus.FAILED
                cls._jobs[job_id].error = str(e)
                cls._jobs[job_id].completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                update_scrape_run(job_id=job_id, status='FAILED', error_message=str(e))
                log_action(action='job_crash', session_id=params.session_id, details={'job_id': job_id, 'error': str(e)})

        # Start the thread
        thread = threading.Thread(target=worker, daemon=True, name=f"ScrapeJob-{job_id}")
        cls._threads[job_id] = thread
        thread.start()
        
        return job_id

    @classmethod
    def get_job(cls, job_id: str) -> JobState:
        """
        Retrieves the current state of a job.
        """
        return cls._jobs.get(job_id)

    @classmethod
    def cancel_job(cls, job_id: str):
        """
        Signals a running job to terminate early.
        """
        if job_id in cls._cancellation_events:
            cls._cancellation_events[job_id].set()
            
    @classmethod
    def clear_job(cls, job_id: str):
        """
        Cleans up memory for a completed or cancelled job.
        """
        if job_id in cls._jobs:
            del cls._jobs[job_id]
        if job_id in cls._threads:
            del cls._threads[job_id]
        if job_id in cls._cancellation_events:
            del cls._cancellation_events[job_id]
