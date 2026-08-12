"""Regression tests for the SEO expiry gate.

The zammejobs.com deindexing (indexed pages 21k → 3k, Jun–Aug 2026) was caused
by expired job postings still being served as live, indexable 200 pages and
kept in the sitemap. These tests pin the predicate that now gates that.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace

from src.api.frontend import _is_expired_job


def _job(status="active", date_expires=None):
    return SimpleNamespace(status=status, date_expires=date_expires)


def test_active_job_with_no_expiry_is_live():
    assert _is_expired_job(_job()) is False


def test_active_job_with_future_expiry_is_live():
    assert _is_expired_job(_job(date_expires=datetime.utcnow() + timedelta(days=5))) is False


def test_active_job_with_past_expiry_is_expired():
    assert _is_expired_job(_job(date_expires=datetime.utcnow() - timedelta(days=1))) is True


def test_status_expired_is_expired_even_without_date():
    assert _is_expired_job(_job(status="expired")) is True


def test_status_inactive_is_expired():
    assert _is_expired_job(_job(status="inactive")) is True
