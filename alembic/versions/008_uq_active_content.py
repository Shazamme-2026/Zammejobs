"""Structural guard: at most one ACTIVE row per (source_type, content_hash).

The Shazamme feed re-lists the same posting under a fresh source_id (GUID)
every pull and repeats jobs within a single pull. Keying identity on
(source_type, source_id) let one job accumulate as thousands of active rows
(129,371 active for ~31,240 real jobs). Ingestion now dedups on content_hash,
but a partial unique index makes it *physically impossible* for any code path —
present or future — to store two active copies of the same job.

This migration is idempotent and self-cleaning:
  1. Collapse existing active duplicates, keeping the newest row per
     (source_type, content_hash) and expiring the rest.
  2. Create the partial unique index the dedup relies on.

Revision ID: 008
Revises: 007
"""

from typing import Union

from alembic import op


revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Collapse active dupes so the unique index can be built. Keep the most
    #    recently crawled row per (source_type, content_hash); expire the twins.
    op.execute(
        """
        WITH ranked AS (
            SELECT id, row_number() OVER (
                PARTITION BY source_type, content_hash
                ORDER BY date_crawled DESC NULLS LAST, id
            ) AS rn
            FROM jobs
            WHERE status = 'active'
        )
        UPDATE jobs
        SET status = 'expired', date_updated = now()
        WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
        """
    )

    # 2. Enforce it structurally. Partial index => only active rows are
    #    constrained; historical expired/hidden dupes are left untouched.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_active_content
        ON jobs (source_type, content_hash)
        WHERE status = 'active'
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_active_content")
