"""Celery tasks for crawling, normalization, and cleanup.

Features:
- Circuit breaker: pauses sources after consecutive failures
- Structured logging for all crawl operations
- Error classification: retryable vs permanent
- Crawl metrics tracking
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from celery import Celery
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.config import settings
from src.connectors.base import PermanentError, RateLimitError, RawJob
from src.connectors.adzuna import AdzunaConnector
from src.connectors.arbeitnow import ArbeitnowConnector
from src.connectors.ashby import AshbyConnector
from src.connectors.bullhorn import BullhornConnector
from src.connectors.bundesagentur import BundesagenturConnector
from src.connectors.canada_jobbank import CanadaJobBankConnector
from src.connectors.careerjet import CareerjetConnector
from src.connectors.eures import EuresConnector
from src.connectors.greenhouse import GreenhouseConnector
from src.connectors.icims import iCIMSConnector
from src.connectors.jooble import JoobleConnector
from src.connectors.lever import LeverConnector
from src.connectors.personio import PersonioConnector
from src.connectors.recruitee import RecruiteeConnector
from src.connectors.reed import ReedConnector
from src.connectors.remoteok import RemoteOKConnector
from src.connectors.remotive import RemotiveConnector
from src.connectors.shazamme import ShazammeConnector
from src.connectors.smartrecruiters import SmartRecruitersConnector
from src.connectors.successfactors import SuccessFactorsConnector
from src.connectors.taleo import TaleoConnector
from src.connectors.themuse import TheMuseConnector
from src.connectors.usajobs import USAJobsConnector
from src.connectors.workable import WorkableConnector
from src.connectors.workday import WorkdayConnector
from src.models import CrawlRun, Job, SourceConfig
from src.normalizer.pipeline import normalize_job

logger = logging.getLogger("jobindex.crawl")


def _naive_utc(dt):
    """Normalize a possibly tz-aware datetime to naive UTC.

    timestamptz columns (last_crawl_at, last_sent_at) come back tz-aware, but
    this module compares against naive datetime.utcnow(). Mixing the two raises
    'can't compare offset-naive and offset-aware datetimes', which silently
    aborted every crawl/alert run.
    """
    if dt is not None and dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

celery_app = Celery("job_ingestion", broker=settings.redis_url, backend=settings.redis_url)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_soft_time_limit=300,
    task_time_limit=600,
)

CONNECTOR_MAP = {
    # ATS direct APIs (highest quality — structured data from source)
    "greenhouse_api": GreenhouseConnector,
    "lever_api": LeverConnector,
    "workday_feed": WorkdayConnector,
    "ashby_api": AshbyConnector,
    "workable_api": WorkableConnector,
    "smartrecruiters_sitemap": SmartRecruitersConnector,
    "recruitee_api": RecruiteeConnector,
    "personio_xml": PersonioConnector,
    # JS-rendered enterprise ATS (hidden API + sitemap + HTML scrape)
    "icims_portal": iCIMSConnector,
    "successfactors_sitemap": SuccessFactorsConnector,
    "taleo_html": TaleoConnector,
    # Staffing-platform ATS
    "bullhorn_api": BullhornConnector,
    # Aggregator APIs (volume — millions of jobs)
    "adzuna_api": AdzunaConnector,
    "remoteok_api": RemoteOKConnector,
    "remotive_api": RemotiveConnector,
    "arbeitnow_api": ArbeitnowConnector,
    "themuse_api": TheMuseConnector,
    "usajobs_api": USAJobsConnector,
    "reed_api": ReedConnector,
    "jooble_api": JoobleConnector,
    "careerjet_api": CareerjetConnector,
    "canada_jobbank_xml": CanadaJobBankConnector,
    "eures_api": EuresConnector,
    "bundesagentur_api": BundesagenturConnector,
    # Shazamme — first-party feed of all tenant jobs
    "shazamme_feed": ShazammeConnector,
}

# Circuit breaker: max consecutive failures before pausing a source
MAX_CONSECUTIVE_FAILURES = 5
CIRCUIT_BREAKER_COOLDOWN_HOURS = 24


def _run_async(coro):
    """Run an async function from sync Celery task."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _get_sync_session():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(settings.database_url_sync)
    return sessionmaker(bind=engine)()


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def crawl_source(self, source_config_id: str):
    """Crawl a single source configuration and store results.

    Circuit breaker: if the source has failed MAX_CONSECUTIVE_FAILURES times
    in a row, skip it and log a warning.
    """
    session = _get_sync_session()

    try:
        config = session.get(SourceConfig, uuid.UUID(source_config_id))
        if not config or not config.is_active:
            logger.info("Skipping source %s: inactive or not found", source_config_id)
            return {"status": "skipped", "reason": "inactive or not found"}

        if settings.shazamme_only_ingestion and config.source_type != "shazamme_feed":
            logger.info(
                "Skipping source %s (%s): shazamme_only_ingestion=True",
                source_config_id, config.source_type,
            )
            return {"status": "skipped", "reason": "shazamme_only_ingestion"}

        # Circuit breaker check
        if _is_circuit_open(session, config):
            logger.warning("Circuit breaker OPEN for source %s (%s). Skipping.", source_config_id, config.source_type)
            return {"status": "skipped", "reason": "circuit breaker open"}

        crawl_run = CrawlRun(
            id=uuid.uuid4(),
            source_config_id=config.id,
            status="running",
        )
        session.add(crawl_run)
        session.commit()

        start_time = datetime.utcnow()
        source_desc = f"{config.source_type}:{config.config.get('board_token', '?')}"
        logger.info("Starting crawl for %s", source_desc)

        try:
            connector_cls = CONNECTOR_MAP.get(config.source_type)
            if not connector_cls:
                raise ValueError(f"Unknown source type: {config.source_type}")

            raw_jobs = _run_async(_fetch_with_connector(connector_cls, config.source_type, config.config or {}))

            # Normalize and upsert
            jobs_new = 0
            jobs_updated = 0
            new_urls: list[str] = []
            seen_source_ids: set[str] = set()
            prev_job_count = config.last_crawl_job_count or 0

            for raw_job in raw_jobs:
                try:
                    job = _run_async(normalize_job(raw_job, do_geocode=False))
                    if job.source_id:
                        seen_source_ids.add(job.source_id)
                    result = _upsert_job(session, job)
                    if result == "new":
                        jobs_new += 1
                        new_urls.append(f"https://{settings.site_domain}/jobs/{job.id}")
                    else:
                        jobs_updated += 1
                except Exception as e:
                    logger.warning("Failed to normalize/upsert job from %s: %s", source_desc, str(e)[:100])

            session.commit()

            # Feed-snapshot reconciliation. A Shazamme feed pull is a COMPLETE
            # snapshot of currently-open jobs, so any active shazamme_feed job
            # whose source_id is absent from this pull has left the feed and is
            # expired. Without this the hourly crawler only ever upserts, so
            # closed jobs linger until the 30-day mark_stale_jobs timer —
            # inflating the active index (Aequor showed 32.5K vs 9.3K live).
            # Guarded by _should_reconcile so a truncated feed can't mass-expire.
            if config.source_type == "shazamme_feed" and seen_source_ids:
                if _should_reconcile(prev_job_count, len(raw_jobs), settings.reconcile_min_ratio):
                    expired_ids = _reconcile_feed_snapshot(
                        session, config.source_type, seen_source_ids
                    )
                    if expired_ids:
                        logger.info(
                            "Reconcile: expired %d %s jobs no longer in feed",
                            len(expired_ids), source_desc,
                        )
                        _notify_jobs_deleted(expired_ids)
                else:
                    logger.warning(
                        "Reconcile SKIPPED for %s: pull %d < %.0f%% of last good %d "
                        "(suspected feed truncation)",
                        source_desc, len(raw_jobs),
                        settings.reconcile_min_ratio * 100, prev_job_count,
                    )

            # Push new URLs to IndexNow + Google Indexing API. Best-effort —
            # failures don't fail the crawl.
            if new_urls:
                try:
                    from src.indexing.indexnow import submit_urls as indexnow_submit
                    submitted = _run_async(indexnow_submit(new_urls))
                    if submitted:
                        logger.info("IndexNow: submitted %d new job URLs", submitted)
                except Exception as e:
                    logger.warning("IndexNow dispatch failed: %s", str(e)[:120])
                try:
                    from src.indexing.google import notify_url_updated
                    # Google Indexing free quota is 200/day, so cap per-crawl
                    for u in new_urls[:200]:
                        _run_async(notify_url_updated(u))
                except Exception as e:
                    logger.warning("Google Indexing dispatch failed: %s", str(e)[:120])

            # Update crawl run
            duration = (datetime.utcnow() - start_time).total_seconds()
            crawl_run.status = "success"
            crawl_run.completed_at = datetime.utcnow()
            crawl_run.jobs_found = len(raw_jobs)
            crawl_run.jobs_new = jobs_new
            crawl_run.jobs_updated = jobs_updated
            crawl_run.duration_seconds = duration

            config.last_crawl_at = datetime.utcnow()
            config.last_crawl_status = "success"
            config.last_crawl_job_count = len(raw_jobs)

            session.commit()

            logger.info(
                "Crawl complete for %s: %d found, %d new, %d updated (%.1fs)",
                source_desc, len(raw_jobs), jobs_new, jobs_updated, duration,
            )

            # When a Shazamme crawl lands the first batch of rows, flip the
            # visibility gate so non-Shazamme jobs get hidden without waiting
            # for the next web boot.
            if (
                config.source_type == "shazamme_feed"
                and settings.shazamme_only_ingestion
                and jobs_new > 0
            ):
                try:
                    from scripts.sync_shazamme_visibility import main as sync_visibility
                    sync_visibility()
                except Exception as e:
                    logger.warning("Post-crawl visibility sync failed: %s", str(e)[:200])

            return {
                "status": "success",
                "source": source_desc,
                "jobs_found": len(raw_jobs),
                "jobs_new": jobs_new,
                "jobs_updated": jobs_updated,
                "duration_seconds": round(duration, 1),
            }

        except PermanentError as e:
            _record_failure(session, crawl_run, config, start_time, str(e), permanent=True)
            logger.error("Permanent error crawling %s: %s", source_desc, e)
            return {"status": "permanent_error", "source": source_desc, "error": str(e)}

        except RateLimitError as e:
            _record_failure(session, crawl_run, config, start_time, str(e), permanent=False)
            logger.warning("Rate limited crawling %s: %s", source_desc, e)
            # Retry with longer delay
            raise self.retry(exc=e, countdown=e.retry_after or 120)

        except Exception as e:
            _record_failure(session, crawl_run, config, start_time, str(e), permanent=False)
            logger.error("Error crawling %s: %s", source_desc, str(e)[:200])
            raise self.retry(exc=e)

    finally:
        session.close()


def _upsert_job(session, job: Job) -> str:
    """Insert or update a job. Returns 'new' or 'updated'."""
    stmt = insert(Job).values(
        id=job.id,
        content_hash=job.content_hash,
        source_type=job.source_type,
        source_id=job.source_id,
        source_url=job.source_url,
        ats_platform=job.ats_platform,
        title=job.title,
        description_html=job.description_html,
        description_text=job.description_text,
        employer_name=job.employer_name,
        employer_domain=job.employer_domain,
        employer_logo_url=job.employer_logo_url,
        location_raw=job.location_raw,
        location_city=job.location_city,
        location_state=job.location_state,
        location_country=job.location_country,
        location_lat=job.location_lat,
        location_lng=job.location_lng,
        is_remote=job.is_remote,
        remote_type=job.remote_type,
        salary_min=job.salary_min,
        salary_max=job.salary_max,
        salary_currency=job.salary_currency,
        salary_period=job.salary_period,
        salary_raw=job.salary_raw,
        employment_type=job.employment_type,
        categories=job.categories,
        seniority=job.seniority,
        date_posted=job.date_posted,
        date_expires=job.date_expires,
        date_crawled=job.date_crawled,
        date_updated=job.date_updated,
        status=job.status,
        raw_data=job.raw_data,
    ).on_conflict_do_update(
        constraint="uq_source",
        set_={
            "title": job.title,
            "description_html": job.description_html,
            "description_text": job.description_text,
            "location_raw": job.location_raw,
            "location_city": job.location_city,
            "location_state": job.location_state,
            "location_country": job.location_country,
            "salary_raw": job.salary_raw,
            "salary_min": job.salary_min,
            "salary_max": job.salary_max,
            "date_updated": datetime.utcnow(),
            "status": "active",
            "raw_data": job.raw_data,
        },
    )

    session.execute(stmt)
    return "new"  # PostgreSQL doesn't easily distinguish insert vs update in on_conflict


def _should_reconcile(prev_count: int, curr_count: int, min_ratio: float) -> bool:
    """Truncation guard for feed-snapshot reconciliation.

    A Shazamme feed pull is a complete snapshot, so we normally expire any
    active job missing from it. But the feed intermittently truncates (see
    job-index deep-offset bug), and a short pull must NOT be allowed to expire
    thousands of live jobs. Reconcile only when there's no prior baseline yet,
    or this pull is at least `min_ratio` of the last good pull.
    """
    if not prev_count:
        return True
    return curr_count >= prev_count * min_ratio


def _reconcile_feed_snapshot(session, source_type: str, seen_source_ids: set) -> list:
    """Expire active jobs of `source_type` whose source_id was absent from the
    latest full feed pull. Returns the expired job IDs.

    Only safe for full-snapshot feeds (Shazamme) — never for paginated
    connectors that emit partial per-page results.
    """
    rows = session.execute(
        select(Job.id, Job.source_id)
        .where(Job.source_type == source_type)
        .where(Job.status == "active")
    ).all()
    stale_ids = [r[0] for r in rows if r[1] and r[1] not in seen_source_ids]
    if not stale_ids:
        return []

    CHUNK = 5000
    for i in range(0, len(stale_ids), CHUNK):
        batch = stale_ids[i:i + CHUNK]
        session.execute(
            update(Job)
            .where(Job.id.in_(batch))
            .values(status="expired", date_updated=datetime.utcnow())
        )
    session.commit()
    return stale_ids


def _record_failure(session, crawl_run, config, start_time, error_msg, permanent=False):
    """Record a failed crawl run."""
    crawl_run.status = "failed"
    crawl_run.completed_at = datetime.utcnow()
    crawl_run.error_message = error_msg[:500]
    crawl_run.duration_seconds = (datetime.utcnow() - start_time).total_seconds()

    config.last_crawl_at = datetime.utcnow()
    config.last_crawl_status = "permanent_error" if permanent else "failed"

    session.commit()


def _is_circuit_open(session, config: SourceConfig) -> bool:
    """Check if the circuit breaker is open (too many consecutive failures)."""
    recent_runs = session.execute(
        select(CrawlRun)
        .where(CrawlRun.source_config_id == config.id)
        .order_by(CrawlRun.started_at.desc())
        .limit(MAX_CONSECUTIVE_FAILURES)
    ).scalars().all()

    if len(recent_runs) < MAX_CONSECUTIVE_FAILURES:
        return False

    all_failed = all(r.status in ("failed", "permanent_error") for r in recent_runs)
    if not all_failed:
        return False

    # Check if cooldown has passed
    most_recent = recent_runs[0]
    if most_recent.completed_at:
        cooldown_end = _naive_utc(most_recent.completed_at) + timedelta(hours=CIRCUIT_BREAKER_COOLDOWN_HOURS)
        if datetime.utcnow() < cooldown_end:
            return True

    return False


async def _fetch_with_connector(connector_cls, source_type: str, config: dict) -> list[RawJob]:
    """All connectors share the (board_token, employer_domain) signature.

    Per-connector meaning of board_token:
      - ATS connectors: the board/company slug (e.g. greenhouse 'spotify', workable subdomain).
      - Adzuna / Careerjet: country code ('us', 'gb', etc).
      - USAJobs / Reed / Jooble / Remotive: optional keyword/category filter.
      - CanadaJobBank: language code ('eng' or 'fra').
      - RemoteOK / Arbeitnow / TheMuse: ignored (single global feed).
    """
    async with connector_cls() as connector:
        return await connector.fetch_jobs(
            config.get("board_token", ""),
            config.get("employer_domain", ""),
        )


@celery_app.task(bind=True, max_retries=2, default_retry_delay=300)
def harvest_common_crawl(
    self,
    crawl_id: str = "2024-12",
    max_files: int = 3,
    max_records: int = 100_000,
):
    """Harvest schema.org JobPosting structured data from a Common Crawl
    snapshot via the Web Data Commons extracts.

    Streaming end-to-end: the .nq.gz files (100MB-2GB each) are downloaded
    chunked to a tempfile and parsed line-by-line, never materialized in
    RAM. RawJobs are normalized and upserted as they are emitted.

    Returns dict with files_processed, jobs_seen, jobs_inserted,
    jobs_updated, jobs_skipped_duplicate.
    """
    from src.harvest.common_crawl import iter_raw_jobs

    logger.info(
        "Common Crawl harvest start: crawl_id=%s max_files=%d max_records=%d",
        crawl_id, max_files, max_records,
    )

    session = _get_sync_session()
    new_urls: list[str] = []
    jobs_seen = 0
    jobs_inserted = 0
    jobs_updated = 0
    jobs_skipped_duplicate = 0
    files_processed = 0

    # Preload the set of URLs we already have for this source. This is the
    # cheap dedupe short-circuit: if a URL is already in the DB we skip the
    # entire normalize -> upsert path. For a Common Crawl run the table can
    # have millions of rows, so we cap at the most recent N to keep memory
    # bounded; anything outside that window will fall through to the
    # on_conflict_do_update on uq_source and still be safe, just not as fast.
    DEDUPE_WINDOW = 2_000_000
    try:
        rows = session.execute(
            select(Job.source_url)
            .where(Job.source_type == "commoncrawl_jobposting")
            .order_by(Job.date_updated.desc())
            .limit(DEDUPE_WINDOW)
        ).all()
        seen_urls: set[str] = {r[0] for r in rows if r[0]}
        logger.info("Common Crawl dedupe set: %d known URLs", len(seen_urls))
    except Exception as e:
        logger.warning("Could not preload dedupe set: %s", str(e)[:120])
        seen_urls = set()

    async def _drive():
        nonlocal jobs_seen, jobs_inserted, jobs_updated, jobs_skipped_duplicate, files_processed
        last_file_url: str | None = None

        async for raw in iter_raw_jobs(
            crawl_id=crawl_id,
            max_files=max_files,
            max_records=max_records,
            seen_urls=seen_urls,
        ):
            # `iter_raw_jobs` already short-circuited on `seen_urls`, but a
            # secondary check guards against any race between preload and now.
            jobs_seen += 1

            # Track distinct files so we can report files_processed.
            file_marker = raw.raw_data.get("graph_url") if raw.raw_data else None
            if file_marker and file_marker != last_file_url:
                last_file_url = file_marker

            if raw.source_url in seen_urls:
                jobs_skipped_duplicate += 1
                continue

            try:
                job = await normalize_job(raw, do_geocode=False)
                _upsert_job(session, job)
                seen_urls.add(raw.source_url)
                jobs_inserted += 1
                new_urls.append(f"https://{settings.site_domain}/jobs/{job.id}")
                # Commit periodically so we don't lose everything on a crash.
                if jobs_inserted % 1_000 == 0:
                    session.commit()
                    logger.info(
                        "Common Crawl progress: seen=%d inserted=%d skipped=%d",
                        jobs_seen, jobs_inserted, jobs_skipped_duplicate,
                    )
            except Exception as e:
                logger.warning(
                    "Common Crawl normalize/upsert failed for %s: %s",
                    raw.source_url, str(e)[:120],
                )

        # Approximate files_processed from max_files cap; the streaming API
        # drains files in order and stops on max_records.
        files_processed = max_files

    try:
        _run_async(_drive())
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("Common Crawl harvest failed: %s", str(e)[:200])
        raise self.retry(exc=e)
    finally:
        session.close()

    # Push new URLs to IndexNow + Google Indexing API. Best-effort.
    if new_urls:
        try:
            from src.indexing.indexnow import submit_urls as indexnow_submit
            submitted = _run_async(indexnow_submit(new_urls))
            if submitted:
                logger.info("IndexNow: submitted %d Common Crawl URLs", submitted)
        except Exception as e:
            logger.warning("IndexNow dispatch failed: %s", str(e)[:120])
        try:
            from src.indexing.google import notify_url_updated
            # Google Indexing free quota is 200/day, so cap per-run.
            for u in new_urls[:200]:
                _run_async(notify_url_updated(u))
        except Exception as e:
            logger.warning("Google Indexing dispatch failed: %s", str(e)[:120])

    result = {
        "files_processed": files_processed,
        "jobs_seen": jobs_seen,
        "jobs_inserted": jobs_inserted,
        "jobs_updated": jobs_updated,
        "jobs_skipped_duplicate": jobs_skipped_duplicate,
    }
    logger.info("Common Crawl harvest complete: %s", result)
    return result


@celery_app.task
def mark_stale_jobs():
    """Mark jobs as expired if not updated in `settings.stale_job_days` days.

    Loosened from 7 → 30 days because the stale threshold was triggering on
    seed data that hadn't yet been re-crawled by the auto-pipeline. The
    correct freshness signal is per-source `last_crawl_at` + per-source
    interval (handled in crawl_all_due_sources), not a global cutoff.

    This window is the single source of truth for the expiry model: the
    JSON-LD validThrough for jobs without an explicit source expiry is derived
    from the same date_updated + stale_job_days, so a still-crawled listing
    never advertises a lapsed expiry to Google.
    """
    session = _get_sync_session()
    try:
        threshold = datetime.utcnow() - timedelta(days=settings.stale_job_days)

        # Snapshot the IDs being expired so we can notify Google + IndexNow.
        from sqlalchemy import select as sa_select
        expiring_ids = [
            r[0] for r in session.execute(
                sa_select(Job.id)
                .where(Job.status == "active")
                .where(Job.date_updated < threshold)
            ).all()
        ]

        stmt = (
            update(Job)
            .where(Job.status == "active")
            .where(Job.date_updated < threshold)
            .values(status="expired", date_updated=datetime.utcnow())
        )

        result = session.execute(stmt)
        session.commit()

        expired_count = result.rowcount
        logger.info("Marked %d stale jobs as expired", expired_count)

        if expiring_ids:
            _notify_jobs_deleted(expiring_ids)

        return {"jobs_expired": expired_count}
    finally:
        session.close()


def _notify_jobs_deleted(job_ids: list) -> None:
    """Best-effort: tell Google Indexing API + IndexNow that JobPosting URLs
    are gone. Caps Google calls to its 200/day free quota; IndexNow has no
    practical limit so we submit all of them in one batch.
    """
    if not job_ids:
        return
    urls = [f"https://{settings.site_domain}/jobs/{jid}" for jid in job_ids]
    try:
        from src.indexing.indexnow import submit_urls as indexnow_submit
        _run_async(indexnow_submit(urls))
    except Exception as e:
        logger.warning("IndexNow expiry dispatch failed: %s", str(e)[:120])
    try:
        from src.indexing.google import notify_url_deleted
        for u in urls[:200]:
            _run_async(notify_url_deleted(u))
    except Exception as e:
        logger.warning("Google Indexing expiry dispatch failed: %s", str(e)[:120])


@celery_app.task
def validate_active_job_urls(sample_size: int = 500):
    """HEAD-check a random sample of active jobs. Mark any that 404/410 as expired.

    Catches jobs filled between full crawls without waiting 7 days for
    mark_stale_jobs to expire them.

    Skipped entirely when shazamme_only_ingestion is on: many Shazamme
    tenant sites sit behind Cloudflare and block HEAD / bot user-agents,
    so the check produces false-positives and would silently expire
    valid jobs. The Shazamme daily feed handles lifecycle.
    """
    if settings.shazamme_only_ingestion:
        logger.info("validate_active_job_urls: skipped (shazamme_only_ingestion on)")
        return {"checked": 0, "expired": 0, "skipped_reason": "shazamme_only_ingestion"}

    import asyncio
    import aiohttp
    from sqlalchemy import select
    from sqlalchemy.sql.expression import func as sql_func

    session = _get_sync_session()
    try:
        rows = session.execute(
            select(Job.id, Job.source_url)
            .where(Job.status == "active")
            .where(Job.source_url.isnot(None))
            .order_by(sql_func.random())
            .limit(sample_size)
        ).all()
    finally:
        session.close()

    if not rows:
        return {"checked": 0, "expired": 0}

    async def head(session_h, job_id, url):
        try:
            async with session_h.head(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status in (404, 410, 451):
                    return ("dead", job_id)
                if r.status in (405, 501):
                    async with session_h.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=8)) as g:
                        return ("dead" if g.status in (404, 410, 451) else "ok", job_id)
                return ("ok", job_id)
        except Exception:
            return ("error", job_id)

    async def run():
        sem = asyncio.Semaphore(20)
        async with aiohttp.ClientSession(headers={"User-Agent": settings.bot_user_agent}) as s:
            async def bounded(jid, u):
                async with sem:
                    return await head(s, jid, u)
            return await asyncio.gather(*(bounded(jid, u) for jid, u in rows))

    results = _run_async(run())
    dead_ids = [r[1] for r in results if r[0] == "dead"]

    if dead_ids:
        session = _get_sync_session()
        try:
            session.execute(
                update(Job).where(Job.id.in_(dead_ids)).values(status="expired", date_updated=datetime.utcnow())
            )
            session.commit()
        finally:
            session.close()
        _notify_jobs_deleted(dead_ids)

    logger.info("Liveness sweep: %d checked, %d marked expired", len(rows), len(dead_ids))
    return {"checked": len(rows), "expired": len(dead_ids)}


@celery_app.task
def backfill_coords(batch_size: int = 5000):
    """Resolve location_city + location_country to lat/lng via GeoNames cities500.
    Idempotent — only touches rows where lat/lng is currently NULL.

    Runs every 30 min; 5K rows/batch keeps the worker responsive. After a
    fresh seed, it'll take ~12 batches to backfill 60K jobs.
    """
    from sqlalchemy import text as sql_text
    from src.geo.cities import lookup as city_lookup

    session = _get_sync_session()
    try:
        rows = session.execute(sql_text("""
            SELECT id, location_city, location_country
              FROM jobs
             WHERE status = 'active'
               AND location_lat IS NULL
               AND location_city IS NOT NULL
               AND location_city != ''
               AND location_country IS NOT NULL
             LIMIT :limit
        """), {"limit": batch_size}).all()

        filled = 0
        for jid, city, country in rows:
            coords = city_lookup(country, city)
            if coords:
                lat, lng = coords
                session.execute(sql_text("""
                    UPDATE jobs SET location_lat = :lat, location_lng = :lng
                     WHERE id = :id
                """), {"lat": lat, "lng": lng, "id": jid})
                filled += 1
        session.commit()
        logger.info("backfill_coords: %d/%d rows resolved", filled, len(rows))
        return {"checked": len(rows), "resolved": filled}
    finally:
        session.close()


@celery_app.task
def link_employers():
    """Auto-create Employer rows for any (employer_domain, employer_name) pair
    seen in jobs but not yet in the employer table, then link Job.employer_id.

    Runs every 30 min via Celery beat. Idempotent. Catches Bullhorn customers,
    Shazamme tenants, aggregator-discovered companies, etc.
    """
    from sqlalchemy import text as sql_text
    session = _get_sync_session()
    try:
        # 1. Auto-create new employer rows for unlinked jobs
        r1 = session.execute(sql_text("""
            INSERT INTO employers (id, name, domain, ats_platform, country, created_at, updated_at)
            SELECT
              gen_random_uuid(),
              MAX(j.employer_name),
              lower(j.employer_domain),
              MAX(j.ats_platform),
              MAX(j.location_country),
              NOW(),
              NOW()
            FROM jobs j
            LEFT JOIN employers e ON lower(e.domain) = lower(j.employer_domain)
            WHERE j.employer_id IS NULL
              AND j.employer_domain IS NOT NULL
              AND j.employer_domain != ''
              AND e.id IS NULL
            GROUP BY lower(j.employer_domain)
            ON CONFLICT (domain) DO NOTHING
        """))
        created = r1.rowcount

        # 2. Link unlinked jobs to their employer by domain match
        r2 = session.execute(sql_text("""
            UPDATE jobs j
               SET employer_id = e.id
              FROM employers e
             WHERE j.employer_id IS NULL
               AND lower(j.employer_domain) = lower(e.domain)
        """))
        linked = r2.rowcount
        session.commit()

        logger.info("link_employers: created %d new employers, linked %d jobs", created, linked)
        return {"employers_created": created, "jobs_linked": linked}
    finally:
        session.close()


@celery_app.task
def crawl_all_due_sources():
    """Find all source_configs that are due for crawling and dispatch crawl_source.

    Runs every 30 min via Celery beat. The beat schedule registers this as
    `src.tasks.crawl.crawl_all_due_sources` — this module path matters because
    Celery workers only know tasks they've imported, and the worker imports
    src.tasks.crawl (not src.scheduler).
    """
    session = _get_sync_session()
    try:
        query = select(SourceConfig).where(SourceConfig.is_active == True)
        if settings.shazamme_only_ingestion:
            query = query.where(SourceConfig.source_type == "shazamme_feed")
        configs = session.execute(query).scalars().all()

        dispatched = 0
        for config in configs:
            if config.last_crawl_at is None:
                crawl_source.delay(str(config.id))
                dispatched += 1
            else:
                next_crawl = _naive_utc(config.last_crawl_at) + timedelta(hours=config.crawl_interval_hours)
                if datetime.utcnow() >= next_crawl:
                    crawl_source.delay(str(config.id))
                    dispatched += 1

        logger.info(
            "crawl_all_due_sources: dispatched %d / %d source_configs (shazamme_only=%s)",
            dispatched, len(configs), settings.shazamme_only_ingestion,
        )
        return {
            "dispatched": dispatched,
            "total_configs": len(configs),
            "shazamme_only": settings.shazamme_only_ingestion,
        }
    finally:
        session.close()


@celery_app.task
def send_due_alerts():
    """For each confirmed, active JobAlert whose cadence is due, find new
    matching jobs since last_sent_at and email a digest. Idempotent on
    re-run (last_sent_at updates within the same transaction)."""
    import asyncio

    from src.api.frontend import _canonical_url
    from src.mcp_server.nl_parser import parse_natural_language
    from src.models import JobAlert
    from src.services.email import send_email, signed_token

    session = _get_sync_session()
    sent = 0
    skipped = 0
    try:
        cadence_hours = {"instant": 0, "daily": 24, "weekly": 168}
        now = datetime.utcnow()

        alerts = session.execute(
            select(JobAlert).where(JobAlert.is_active == True, JobAlert.is_confirmed == True)
        ).scalars().all()

        for alert in alerts:
            interval = cadence_hours.get(alert.cadence, 24)
            if alert.last_sent_at is not None:
                next_due = _naive_utc(alert.last_sent_at) + timedelta(hours=interval)
                if now < next_due:
                    skipped += 1
                    continue

            since = alert.last_sent_at or (now - timedelta(days=7))
            parsed = parse_natural_language(alert.query)

            clauses = [Job.status == "active", Job.date_crawled >= since]
            if parsed.keywords:
                pat = f"%{parsed.keywords}%"
                clauses.append(Job.title.ilike(pat) | Job.description_text.ilike(pat))
            if parsed.country:
                clauses.append(Job.location_country == parsed.country.upper())
            if parsed.city:
                cpat = f"%{parsed.city}%"
                clauses.append(Job.location_city.ilike(cpat) | Job.location_raw.ilike(cpat))
            if parsed.is_remote is True:
                clauses.append(Job.is_remote == True)
            if parsed.salary_min:
                clauses.append(Job.salary_max >= parsed.salary_min)

            new_jobs = session.execute(
                select(Job).where(*clauses)
                .order_by(Job.date_posted.desc().nullslast())
                .limit(15)
            ).scalars().all()

            session.execute(
                update(JobAlert).where(JobAlert.id == alert.id)
                .values(last_sent_at=now, last_match_count=len(new_jobs))
            )
            session.commit()

            if not new_jobs:
                continue

            unsub = signed_token({"alert_id": str(alert.id), "purpose": "unsubscribe"})
            unsub_url = _canonical_url(f"/alerts/unsubscribe?token={unsub}")
            cards_html = "\n".join(
                f"""<a href="{_canonical_url(f"/jobs/{j.id}")}" style="display:block;padding:14px 16px;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:8px;text-decoration:none;color:#111;">
<div style="font-weight:600;margin-bottom:4px;">{j.title}</div>
<div style="color:#6b7280;font-size:13px;">{j.employer_name}{' · ' + (j.location_city or j.location_country) if (j.location_city or j.location_country) else ''}{' · Remote' if j.is_remote else ''}{' · ' + (j.salary_currency or 'USD') + ' ' + format(j.salary_min, ',') if j.salary_min else ''}</div>
</a>"""
                for j in new_jobs
            )
            html = f"""<!doctype html><html><body style="font-family:-apple-system,sans-serif;max-width:560px;margin:2rem auto;color:#111;">
<h2 style="color:#EC008C;">{len(new_jobs)} new match{'es' if len(new_jobs) != 1 else ''} for: {alert.query}</h2>
{cards_html}
<p style="margin-top:24px;color:#6b7280;font-size:12px;">
  ZammeJobs · <a href="https://www.zammejobs.com" style="color:#EC008C;">www.zammejobs.com</a> · <a href="{unsub_url}" style="color:#6b7280;">Unsubscribe</a>
</p>
</body></html>"""

            try:
                asyncio.get_event_loop()
                ok = asyncio.run(send_email(
                    alert.email,
                    f"{len(new_jobs)} new job{'s' if len(new_jobs) != 1 else ''} matching '{alert.query[:60]}'",
                    html,
                ))
            except RuntimeError:
                ok = asyncio.run(send_email(
                    alert.email,
                    f"{len(new_jobs)} new job{'s' if len(new_jobs) != 1 else ''} matching '{alert.query[:60]}'",
                    html,
                ))
            if ok:
                sent += 1

        logger.info("send_due_alerts: sent=%d skipped=%d total=%d", sent, skipped, len(alerts))
        return {"sent": sent, "skipped": skipped, "total": len(alerts)}
    finally:
        session.close()
