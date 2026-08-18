"""Import Shazamme's full LinkedIn-format XML feed into ZammeJobs.

Streams the 250 MB UTF-16 XML, parses each <job>, normalizes, and upserts.
Use this for the initial backfill; the Celery beat schedule should keep
it fresh after that.

Usage:
    python3 -m scripts.import_shazamme
    python3 -m scripts.import_shazamme --url https://other.example.com/feed.xml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.connectors.shazamme import DEFAULT_FEED_URL, ShazammeConnector
from src.models import Job
from src.normalizer.pipeline import normalize_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("import_shazamme")


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_FEED_URL, help="Override feed URL")
    args = p.parse_args()

    async with ShazammeConnector() as c:
        raw_jobs = await c.fetch_jobs(args.url, "")

    logger.info("Fetched %d raw jobs — normalizing + persisting", len(raw_jobs))

    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    new = upd = errs = 0
    new_urls: list[str] = []
    seen_source_ids: set[str] = set()
    seen_content_hashes: set[str] = set()

    # One long-lived session with batched commits instead of a fresh
    # session+commit per job. 38K individual transactions on the shared prod
    # DB (while the web container also serves traffic) is slow enough that the
    # import often never reaches the reconciliation step below. A per-row
    # SAVEPOINT keeps one bad row from losing the whole in-flight batch.
    BATCH = 500
    processed = 0
    s = Session()
    try:
        from sqlalchemy import select as sa_select, update as sa_update
        for raw in raw_jobs:
            if raw.source_id:
                seen_source_ids.add(raw.source_id)
            try:
                job = await normalize_job(raw, do_geocode=False)
            except Exception as e:
                errs += 1
                logger.warning("Normalize failed: %s", str(e)[:150])
                continue

            if job.content_hash:
                seen_content_hashes.add(job.content_hash)

            try:
                # Dedup on CONTENT, not source_id. The feed re-lists the same
                # posting under a fresh GUID every pull, so a source_id upsert
                # piles up thousands of twins. If an active row already has this
                # content_hash, refresh it in place; otherwise insert (still
                # guarding uq_source for a same-GUID re-pull).
                with s.begin_nested():
                    existing_id = s.execute(
                        sa_select(Job.id)
                        .where(Job.source_type == job.source_type)
                        .where(Job.content_hash == job.content_hash)
                        .where(Job.status == "active")
                        .limit(1)
                    ).scalar()
                    if existing_id is not None:
                        s.execute(
                            sa_update(Job).where(Job.id == existing_id).values(
                                source_url=job.source_url, title=job.title,
                                description_html=job.description_html,
                                salary_raw=job.salary_raw,
                                date_updated=datetime.utcnow(),
                                status="active", raw_data=job.raw_data,
                            )
                        )
                        upd += 1
                    else:
                        stmt = insert(Job).values(
                            id=job.id, content_hash=job.content_hash,
                            source_type=job.source_type, source_id=job.source_id,
                            source_url=job.source_url, ats_platform=job.ats_platform,
                            title=job.title, description_html=job.description_html,
                            description_text=job.description_text,
                            employer_name=job.employer_name, employer_domain=job.employer_domain,
                            employer_logo_url=job.employer_logo_url,
                            location_raw=job.location_raw, location_city=job.location_city,
                            location_state=job.location_state, location_country=job.location_country,
                            is_remote=job.is_remote, remote_type=job.remote_type,
                            salary_min=job.salary_min, salary_max=job.salary_max,
                            salary_currency=job.salary_currency, salary_period=job.salary_period,
                            salary_raw=job.salary_raw, employment_type=job.employment_type,
                            categories=job.categories, seniority=job.seniority,
                            date_posted=job.date_posted, date_expires=job.date_expires,
                            date_crawled=job.date_crawled, date_updated=job.date_updated,
                            status=job.status, raw_data=job.raw_data,
                        ).on_conflict_do_update(
                            constraint="uq_source",
                            set_={"title": job.title, "description_html": job.description_html,
                                  "salary_raw": job.salary_raw, "date_updated": datetime.utcnow(),
                                  "status": "active", "raw_data": job.raw_data},
                        )
                        result = s.execute(stmt)
                        if result.inserted_primary_key:
                            new += 1
                            new_urls.append(f"https://{settings.site_domain}/jobs/{job.id}")
                        else:
                            upd += 1
            except Exception as e:
                errs += 1
                logger.warning("Upsert failed: %s", str(e)[:150])
                continue

            processed += 1
            if processed % BATCH == 0:
                s.commit()
                logger.info("  progress: %d processed (%d new, %d upd, %d err)",
                            processed, new, upd, errs)
        s.commit()
    finally:
        s.close()

    logger.info("Shazamme import done: %d new, %d updated, %d errors", new, upd, errs)

    # Lifecycle: any active shazamme_feed job NOT in this fetch is no longer
    # live. Mark expired + notify Google/IndexNow so the URL drops out.
    #
    # Truncation guard: the Shazamme feed intermittently truncates and
    # self-heals (job-index deep-offset bug). Only reconcile when the pull is
    # a plausibly-complete snapshot — a short feed must NOT mass-expire live
    # jobs. Healthy feed ~38K; the flap drops to ~6K, below reconcile_min_jobs.
    expired_urls: list[str] = []
    if seen_content_hashes and len(seen_content_hashes) < settings.reconcile_min_jobs:
        logger.warning(
            "Reconcile SKIPPED: only %d distinct jobs in feed (< floor %d) — suspected truncation",
            len(seen_content_hashes), settings.reconcile_min_jobs,
        )
    elif seen_content_hashes:
        from sqlalchemy import select, update as sa_update
        from datetime import datetime as _dt
        with Session() as s:
            stale_rows = s.execute(
                select(Job.id, Job.content_hash)
                .where(Job.source_type == "shazamme_feed")
                .where(Job.status == "active")
            ).all()
            stale_ids = [
                row[0] for row in stale_rows
                if row[1] and row[1] not in seen_content_hashes
            ]
            if stale_ids:
                s.execute(
                    sa_update(Job)
                    .where(Job.id.in_(stale_ids))
                    .values(status="expired", date_updated=_dt.utcnow())
                )
                s.commit()
                expired_urls = [
                    f"https://{settings.site_domain}/jobs/{jid}" for jid in stale_ids
                ]
                logger.info("Expired %d Shazamme jobs no longer in feed", len(stale_ids))

    # Flip visibility now that Shazamme jobs are in the DB. Without this
    # the gate only kicks in on the next web boot.
    if new > 0 or upd > 0:
        try:
            from scripts.sync_shazamme_visibility import main as sync_visibility
            sync_visibility()
        except Exception as e:
            logger.warning("Post-import visibility sync failed: %s", str(e)[:200])

    # IndexNow: submit every active job URL (Bing / Yandex / Naver / Seznam
    # + ChatGPT search index, which proxies Bing). Idempotent — re-submission
    # is allowed and just refreshes the lastmod hint.
    try:
        import subprocess
        subprocess.Popen(
            ["python3", "-m", "scripts.indexnow_bulk_submit"],
            stdout=open("/tmp/indexnow_bulk.log", "ab"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        logger.info("Launched indexnow_bulk_submit in background")
    except Exception as e:
        logger.warning("IndexNow dispatch failed: %s", str(e)[:120])

    # Google Indexing API: tell Googlebot to crawl every brand-new job URL,
    # and notify removals for jobs that just dropped out of the feed. Free
    # quota is 200 URL submissions/day per GCP project — split the budget.
    if new_urls or expired_urls:
        try:
            from src.indexing.google import notify_url_deleted, notify_url_updated
            new_count = del_count = 0
            for u in new_urls[:150]:
                if await notify_url_updated(u):
                    new_count += 1
            for u in expired_urls[:50]:
                if await notify_url_deleted(u):
                    del_count += 1
            if new_count or del_count:
                logger.info(
                    "Google Indexing API: %d URL_UPDATED, %d URL_DELETED",
                    new_count, del_count,
                )
        except Exception as e:
            logger.warning("Google Indexing API dispatch failed: %s", str(e)[:120])

    # IndexNow accepts both creates and removals (a 404/410 at the URL is
    # enough for Bing to drop it). Submit expiries in the same batch shape.
    if expired_urls:
        try:
            from src.indexing.indexnow import submit_urls as indexnow_submit
            submitted = await indexnow_submit(expired_urls)
            if submitted:
                logger.info("IndexNow: submitted %d expired URLs", submitted)
        except Exception as e:
            logger.warning("IndexNow expiry dispatch failed: %s", str(e)[:120])


if __name__ == "__main__":
    asyncio.run(main())
