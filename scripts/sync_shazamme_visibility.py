"""Reconcile job visibility with the shazamme_only_ingestion flag.

Guarded logic:
  - flag ON  AND Shazamme rows present   -> hide non-Shazamme (active -> hidden)
  - flag ON  AND no Shazamme rows yet    -> restore hidden -> active (don't
                                            blank the site while the Shazamme
                                            crawl is still pending)
  - flag OFF                             -> restore hidden -> active

All branches are idempotent.
"""

from __future__ import annotations

import sys

from sqlalchemy import create_engine, text

from src.config import settings


def _restore(conn) -> int:
    result = conn.execute(text("""
        UPDATE jobs
        SET status = 'active',
            date_updated = NOW()
        WHERE source_type != 'shazamme_feed'
          AND status = 'hidden'
    """))
    return result.rowcount


def _hide(conn) -> int:
    result = conn.execute(text("""
        UPDATE jobs
        SET status = 'hidden',
            date_updated = NOW()
        WHERE source_type != 'shazamme_feed'
          AND status = 'active'
    """))
    return result.rowcount


def main() -> None:
    engine = create_engine(settings.database_url_sync)
    with engine.begin() as conn:
        # NOTE: this used to blanket-revive EVERY expired shazamme_feed row to
        # 'active' on every boot/crawl to undo a since-removed apply-route
        # liveness bug. Despite the docstring it had no feed-membership check,
        # so it also resurrected legitimately-expired rows: jobs that left the
        # feed AND the duplicate rows that feed-snapshot reconciliation had just
        # expired. That silently neutered reconciliation (active count grew to
        # 129K for ~31K real jobs) and un-deindexed expired SEO pages. The
        # correct liveness signal is now content-hash reconciliation in
        # crawl.py / import_shazamme.py, so the blanket revive is removed.
        if not settings.shazamme_only_ingestion:
            n = _restore(conn)
            print(f"sync_shazamme_visibility: flag OFF, restored {n} jobs", flush=True)
            return

        shazamme_count = conn.execute(text(
            "SELECT COUNT(*) FROM jobs WHERE source_type = 'shazamme_feed'"
        )).scalar() or 0

        if shazamme_count == 0:
            n = _restore(conn)
            print(
                f"sync_shazamme_visibility: no Shazamme rows yet, restored {n} "
                "non-Shazamme jobs so the site isn't blank while the crawl runs",
                flush=True,
            )
            return

        n = _hide(conn)
        print(
            f"sync_shazamme_visibility: Shazamme rows={shazamme_count}, hid {n} non-Shazamme jobs",
            flush=True,
        )
    engine.dispose()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"sync_shazamme_visibility: FAILED — {e}", file=sys.stderr, flush=True)
        sys.exit(0)
