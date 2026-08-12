"""Server-side rendered frontend routes.

Produces SEO-friendly HTML pages with JobPosting JSON-LD schema
for Google Jobs, Bing, and AI search indexing.
"""

from __future__ import annotations

import json
import math
from typing import Optional
from uuid import UUID

import logging
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import UUID

import aiohttp
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.schemas import JobLocation, JobSalary, JobSummary
from src.config import settings
from src.db import get_session
from src.models import Employer, Job
from src.normalizer.salary import currency_for_country


def _job_currency(job: Job) -> str:
    """Resolve a display currency for a job: stored value, then country fallback,
    then USD as last resort. Used so AU jobs aren't tagged USD just because the
    salary string lacked an explicit currency.
    """
    if job.salary_currency:
        return job.salary_currency
    return currency_for_country(job.location_country) or "USD"

logger = logging.getLogger("zammejobs.frontend")

router = APIRouter(tags=["Frontend"])
templates = Jinja2Templates(directory="src/templates")


def _is_expired_job(job: Job) -> bool:
    """A job should not be served as a live, indexable page once it's expired.
    True when the crawler has flipped status away from 'active', or the source's
    explicit expiry has passed. date_expires is a naive UTC column, so compare
    against utcnow(). date_updated-based expiry is handled by mark_stale_jobs
    flipping status, so we don't re-derive it here."""
    if job.status and job.status != "active":
        return True
    if job.date_expires is not None:
        try:
            return job.date_expires < datetime.utcnow()
        except Exception:
            return False
    return False

# Append ?v=<release> to every static asset URL so the browser drops
# its cached copy whenever we ship. src/_version.py is the single
# source of truth — bump it per release.
from src._version import __version__ as _app_version
templates.env.globals["asset_version"] = _app_version


UTM_PARAMS = {
    "source": "zammejobs",
    "utm_source": "zammejobs",
    "utm_medium": "referral",
}


def _append_utm(url: str) -> str:
    """Append source=zammejobs + utm_source/utm_medium to an outbound apply URL.

    Idempotent: if the URL already has a `source` or `utm_source` param, it
    is overwritten with our value rather than duplicated.
    """
    if not url:
        return url
    try:
        parts = urlparse(url)
        if not parts.scheme or not parts.netloc:
            return url
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query.update(UTM_PARAMS)
        return urlunparse(parts._replace(query=urlencode(query)))
    except Exception:
        return url


async def _is_url_alive(url: str, timeout: float = 5.0) -> bool:
    """HEAD-request the URL with a short timeout. Returns True if 2xx/3xx."""
    if not url:
        return False
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
            async with s.head(url, allow_redirects=True) as resp:
                if resp.status < 400:
                    return True
                # Some sites reject HEAD; fall back to a tiny GET
                if resp.status in (405, 501):
                    async with s.get(url, allow_redirects=True) as gresp:
                        return gresp.status < 400
                return False
    except Exception:
        return False


def _employer_fallback_url(job: Job, employer: Employer | None) -> str:
    """Where to send a user when the job's apply URL is dead."""
    if employer and employer.career_page_url:
        return employer.career_page_url
    if employer and employer.domain:
        return f"https://{employer.domain}"
    if job.employer_domain:
        # Strip pseudo-domain suffixes our connectors generate when employer domain is unknown
        d = job.employer_domain
        for suffix in (".adzuna-source.invalid", ".remoteok-source.invalid", ".remotive-source.invalid",
                       ".jooble-source.invalid", ".themuse-source.invalid", ".reed-source.invalid",
                       ".arbeitnow-source.invalid"):
            if d.endswith(suffix):
                return "/"
        return f"https://{d}"
    return "/"


def _canonical_url(path: str) -> str:
    """Build a canonical https://www.zammejobs.com URL. FastAPI's request.url
    sees the upstream HTTP scheme behind Railway's edge so we force https +
    the site_domain rather than reading the request host."""
    from src.config import settings
    host = settings.site_domain or "www.zammejobs.com"
    if not host.startswith("www.") and host == "zammejobs.com":
        host = "www.zammejobs.com"
    return f"https://{host}{path}"


def _build_faqs(job: Job, employer: Employer | None = None) -> list[dict]:
    """Auto-generate FAQ Q&A pairs from job fields. Used both for FAQPage
    JSON-LD (helps Google rich results + AI answer engines) and rendered
    as a visible section on the page. Only includes Qs we can actually
    answer from the data — empty/unknown fields are skipped."""
    faqs: list[dict] = []
    title = job.title
    employer_name = job.employer_name

    faqs.append({
        "q": f"Who is hiring for the {title} role?",
        "a": (
            f"{employer_name} is hiring for the {title} position"
            + (f", a {employer.ats_platform.capitalize()} client" if employer and employer.ats_platform == "shazamme" else "")
            + ". Apply directly on the employer's career site."
        ),
    })

    if job.location_city or job.location_country:
        loc_parts = [p for p in (job.location_city, job.location_state, job.location_country) if p]
        loc_str = ", ".join(loc_parts)
        remote_note = ""
        if job.is_remote:
            remote_note = f" The role is {job.remote_type or 'remote'}-friendly."
        faqs.append({
            "q": f"Where is the {title} job located?",
            "a": f"The {title} role with {employer_name} is based in {loc_str}.{remote_note}",
        })

    if job.is_remote:
        faqs.append({
            "q": f"Is the {title} role remote?",
            "a": (
                f"Yes — the {title} position at {employer_name} is "
                f"{job.remote_type or 'remote'}. "
                + (f"Candidates based in {job.location_country} are preferred." if job.location_country else "")
            ).strip(),
        })

    if job.salary_min or job.salary_max:
        currency = _job_currency(job)
        period = (job.salary_period or "year").lower()
        if job.salary_min and job.salary_max and job.salary_min != job.salary_max:
            band = f"{currency} {job.salary_min:,}–{job.salary_max:,} per {period}"
        elif job.salary_max:
            band = f"up to {currency} {job.salary_max:,} per {period}"
        else:
            band = f"from {currency} {job.salary_min:,} per {period}"
        faqs.append({
            "q": f"What does the {title} role pay?",
            "a": f"{employer_name} lists the {title} role at {band}.",
        })

    if job.employment_type:
        et = job.employment_type.replace("_", " ").lower()
        faqs.append({
            "q": f"Is the {title} role full-time or contract?",
            "a": f"This is a {et} position at {employer_name}.",
        })

    if job.seniority:
        sen = job.seniority.lower()
        faqs.append({
            "q": f"What experience level is the {title} role?",
            "a": f"The {title} position is aimed at {sen}-level candidates.",
        })

    faqs.append({
        "q": f"How do I apply for the {title} role at {employer_name}?",
        "a": (
            f"Apply directly on {employer_name}'s career page via the Apply button on this listing. "
            "ZammeJobs links straight through to the employer's ATS — no third-party form, no resume database."
        ),
    })

    if job.date_posted:
        date_str = job.date_posted.strftime("%B %d, %Y") if hasattr(job.date_posted, "strftime") else str(job.date_posted)[:10]
        faqs.append({
            "q": f"When was the {title} job posted?",
            "a": f"This role was posted on {date_str}.",
        })

    return faqs


def _build_json_ld(job: Job, employer: Employer | None = None) -> str:
    """Build a graph of JSON-LD nodes for a job detail page:

      - JobPosting (Google for Jobs, structured search results)
      - Organization (hiring company)
      - BreadcrumbList (search hierarchy for crawlers + rich results)
      - FAQPage (AEO / GEO — answer engines + Google FAQ rich result)

    Returned as a single @graph document so a single <script> tag covers
    all four entities cleanly. Empty optional fields are omitted to keep
    the JSON-LD lean and avoid Google validator warnings.
    """
    job_canonical = _canonical_url(f"/jobs/{job.id}")
    description = job.description_html or job.description_text or f"{job.title} at {job.employer_name}."

    job_posting: dict = {
        "@type": "JobPosting",
        "@id": f"{job_canonical}#jobposting",
        "title": job.title,
        "description": description,
        "url": job_canonical,
        "identifier": {
            "@type": "PropertyValue",
            "name": job.employer_name,
            "value": str(job.id),
        },
        "hiringOrganization": {
            "@id": f"{job_canonical}#org",
        },
        "employmentType": job.employment_type or "FULL_TIME",
        # Application is completed off-Google on the employer's ATS, so per
        # Google's spec directApply must be false.
        "directApply": False,
    }

    # jobLocation: only emit when we have a country (Google requires at
    # least addressCountry). Drop empty address keys so the validator
    # doesn't flag blank locality/region fields.
    if job.location_country:
        address: dict = {"@type": "PostalAddress", "addressCountry": job.location_country}
        if job.location_city:
            address["addressLocality"] = job.location_city
        if job.location_state:
            address["addressRegion"] = job.location_state
        job_posting["jobLocation"] = {"@type": "Place", "address": address}

    # datePosted is REQUIRED by Google. Fall back to crawl time for sources
    # that don't expose a per-posting date (e.g. Shazamme feed).
    posted = job.date_posted or job.date_crawled
    if posted:
        # ISO 8601 with timezone; Google recommends UTC Z notation.
        try:
            job_posting["datePosted"] = posted.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        except Exception:
            job_posting["datePosted"] = str(posted)[:10]

    # validThrough — prefer the source's explicit expiry. When absent, derive a
    # ROLLING expiry from date_updated (refreshed on every crawl that re-confirms
    # the job live) rather than a fixed posted+N. A fixed posted+30d lapses while
    # the job is demonstrably still in the feed, and a lapsed validThrough makes
    # Google drop the posting from Google Jobs and reclassify it "crawled –
    # currently not indexed". Anchoring on date_updated + stale_job_days matches
    # exactly when mark_stale_jobs would actually expire the job.
    if job.date_expires:
        job_posting["validThrough"] = job.date_expires.strftime("%Y-%m-%dT%H:%M:%S+00:00") if hasattr(job.date_expires, "strftime") else str(job.date_expires)[:10]
    else:
        from datetime import timedelta as _td
        anchor = getattr(job, "date_updated", None) or posted
        if anchor:
            try:
                job_posting["validThrough"] = (anchor + _td(days=settings.stale_job_days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
            except Exception:
                pass
    if job.source_url:
        job_posting["applicantContact"] = {
            "@type": "ContactPoint",
            "url": job.source_url,
        }

    if job.salary_min or job.salary_max:
        value = {"@type": "QuantitativeValue", "unitText": (job.salary_period or "YEAR").upper()}
        if job.salary_min:
            value["minValue"] = job.salary_min
        if job.salary_max:
            value["maxValue"] = job.salary_max
        job_posting["baseSalary"] = {
            "@type": "MonetaryAmount",
            "currency": _job_currency(job),
            "value": value,
        }

    if job.is_remote:
        job_posting["jobLocationType"] = "TELECOMMUTE"
        if job.location_country:
            job_posting["applicantLocationRequirements"] = {
                "@type": "Country",
                "name": job.location_country,
            }

    if job.categories:
        job_posting["industry"] = job.categories[0]
        job_posting["occupationalCategory"] = ", ".join(job.categories[:3])

    if job.seniority:
        job_posting["experienceRequirements"] = {
            "@type": "OccupationalExperienceRequirements",
            "monthsOfExperience": {
                "intern": 0, "junior": 0, "mid": 24, "senior": 60,
                "lead": 84, "principal": 96, "director": 120, "executive": 144,
            }.get(job.seniority.lower() if job.seniority else "", 0),
        }

    org: dict = {
        "@type": "Organization",
        "@id": f"{job_canonical}#org",
        "name": job.employer_name,
    }
    if job.employer_domain:
        org["url"] = f"https://{job.employer_domain}"
        org["sameAs"] = [f"https://{job.employer_domain}"]
    if job.employer_logo_url:
        org["logo"] = job.employer_logo_url

    breadcrumbs = {
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Jobs", "item": _canonical_url("/search")},
            {
                "@type": "ListItem", "position": 2,
                "name": job.employer_name,
                "item": _canonical_url(f"/search?employer={job.employer_domain or ''}"),
            },
            {"@type": "ListItem", "position": 3, "name": job.title, "item": job_canonical},
        ],
    }

    faqs = _build_faqs(job, employer)
    faq_page = {
        "@type": "FAQPage",
        "@id": f"{job_canonical}#faq",
        "mainEntity": [
            {
                "@type": "Question",
                "name": f["q"],
                "acceptedAnswer": {"@type": "Answer", "text": f["a"]},
            }
            for f in faqs
        ],
    }

    extras: list = []

    # SalaryEstimate as a standalone entity so AI engines and Google's
    # rich-result system can cite the figure without re-parsing JobPosting.
    if job.salary_min:
        extras.append({
            "@type": "Occupation",
            "@id": f"{job_canonical}#occupation",
            "name": job.title,
            "estimatedSalary": {
                "@type": "MonetaryAmountDistribution",
                "name": "Salary",
                "currency": _job_currency(job),
                "duration": _ISO8601_DURATION.get((job.salary_period or "year").lower(), "P1Y"),
                "minValue": job.salary_min,
                "maxValue": job.salary_max or job.salary_min,
                "median": int(((job.salary_min or 0) + (job.salary_max or job.salary_min or 0)) / 2),
            },
            "occupationLocation": (
                {"@type": "City", "name": job.location_city} if job.location_city
                else ({"@type": "Country", "name": job.location_country} if job.location_country else None)
            ),
        })

    graph = {
        "@context": "https://schema.org",
        "@graph": [job_posting, org, breadcrumbs, faq_page] + extras,
    }
    return json.dumps(graph, indent=2, ensure_ascii=False)


_ISO8601_DURATION = {"year": "P1Y", "yearly": "P1Y", "annual": "P1Y", "month": "P1M",
                     "monthly": "P1M", "week": "P1W", "weekly": "P1W",
                     "day": "P1D", "daily": "P1D", "hour": "PT1H", "hourly": "PT1H"}


def _job_to_template_obj(job: Job) -> dict:
    """Convert a Job model to a dict suitable for templates."""
    return {
        "id": str(job.id),
        "title": job.title,
        "employer_name": job.employer_name,
        "employer_domain": job.employer_domain,
        "employer_logo_url": job.employer_logo_url,
        "location": {
            "raw": job.location_raw,
            "city": job.location_city,
            "state": job.location_state,
            "country": job.location_country,
        },
        "salary": {
            "min": job.salary_min,
            "max": job.salary_max,
            "currency": _job_currency(job) if (job.salary_min or job.salary_max) else job.salary_currency,
            "period": job.salary_period,
        },
        "employment_type": job.employment_type,
        "seniority": job.seniority,
        "categories": job.categories or [],
        "is_remote": job.is_remote,
        "remote_type": job.remote_type,
        "date_posted": job.date_posted,
        "source_url": job.source_url,
        "ats_platform": job.ats_platform,
        "description_html": job.description_html,
        "description_text": job.description_text,
    }


_COUNTRY_NAMES = {
    "US": "United States", "GB": "United Kingdom", "AU": "Australia",
    "CA": "Canada", "DE": "Germany", "FR": "France", "NL": "Netherlands",
    "IE": "Ireland", "IN": "India", "SG": "Singapore", "BR": "Brazil",
    "JP": "Japan", "ES": "Spain", "IT": "Italy", "MX": "Mexico",
    "NZ": "New Zealand", "PL": "Poland", "ZA": "South Africa",
    "TW": "Taiwan", "HK": "Hong Kong", "IL": "Israel", "CN": "China",
}

_ISO_TO_PATH = {
    "US": "us", "GB": "gb", "AU": "au", "CA": "ca", "DE": "de", "FR": "fr",
    "NL": "nl", "IE": "ie", "IN": "in", "SG": "sg", "BR": "br", "JP": "jp",
    "ES": "es", "IT": "it", "MX": "mx", "NZ": "nz", "PL": "pl", "ZA": "za",
    "TW": "tw", "HK": "hk", "IL": "il", "CN": "cn",
}


@router.get("/", response_class=HTMLResponse)
async def homepage(request: Request, session: AsyncSession = Depends(get_session)):
    # Fetch stats
    total_result = await session.execute(select(func.count()).select_from(Job))
    active_result = await session.execute(
        select(func.count()).select_from(Job).where(Job.status == "active")
    )
    employer_result = await session.execute(select(func.count()).select_from(Employer))

    country_result = await session.execute(
        select(Job.location_country, func.count(Job.id))
        .where(Job.status == "active")
        .group_by(Job.location_country)
        .order_by(func.count(Job.id).desc())
        .limit(20)
    )
    ats_result = await session.execute(
        select(Job.ats_platform, func.count(Job.id))
        .where(Job.status == "active")
        .group_by(Job.ats_platform)
        .order_by(func.count(Job.id).desc())
    )

    # Recent jobs for ticker (5 most recent)
    recent_result = await session.execute(
        select(Job).where(Job.status == "active")
        .order_by(Job.date_posted.desc().nullslast())
        .limit(5)
    )
    recent_jobs = [_job_to_template_obj(j) for j in recent_result.scalars().all()]

    # Top employers by active job count
    job_count_subq = (
        select(Job.employer_id, func.count(Job.id).label("job_count"))
        .where(Job.status == "active")
        .group_by(Job.employer_id)
        .subquery()
    )
    top_emp_result = await session.execute(
        select(Employer, func.coalesce(job_count_subq.c.job_count, 0).label("job_count"))
        .join(job_count_subq, Employer.id == job_count_subq.c.employer_id)
        .order_by(job_count_subq.c.job_count.desc())
        .limit(12)
    )
    top_employers = [
        {
            "id": str(row[0].id),
            "name": row[0].name,
            "domain": row[0].domain,
            "logo_url": row[0].logo_url,
            "ats_platform": row[0].ats_platform,
            "job_count": row[1],
            "initial": (row[0].name[:1] or "?").upper(),
        }
        for row in top_emp_result.all()
    ]

    jobs_by_country = {r[0]: r[1] for r in country_result.all() if r[0]}

    # Build top-8 country cards with names + path slugs
    country_cards = []
    for iso, count in list(jobs_by_country.items())[:8]:
        slug = _ISO_TO_PATH.get(iso)
        if not slug:
            continue
        country_cards.append({
            "iso": iso,
            "slug": slug,
            "name": _COUNTRY_NAMES.get(iso, iso),
            "count": count,
        })

    stats = {
        "total_jobs": total_result.scalar() or 0,
        "active_jobs": active_result.scalar() or 0,
        "total_employers": employer_result.scalar() or 0,
        "jobs_by_country": jobs_by_country,
        "jobs_by_ats": {r[0]: r[1] for r in ats_result.all() if r[0]},
    }

    return templates.TemplateResponse(request, "home.html", {
        "stats": stats,
        "recent_jobs": recent_jobs,
        "top_employers": top_employers,
        "country_cards": country_cards,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    remote: Optional[bool] = None,
    employment_type: Optional[str] = None,
    seniority: Optional[str] = None,
    employer: Optional[str] = None,
    salary_min: Optional[str] = None,  # accept str so empty form value doesn't 422; coerce to int below
    salary_max: Optional[str] = None,
    posted_within: Optional[str] = None,  # "1d", "7d", "30d"
    sort: str = "date",
    page: int = Query(1, ge=1),
    per_page: int = 20,
    session: AsyncSession = Depends(get_session),
):
    from datetime import timedelta

    stmt = select(Job).where(Job.status == "active")
    count_stmt = select(func.count()).select_from(Job).where(Job.status == "active")

    if q:
        kw = or_(Job.title.ilike(f"%{q}%"), Job.description_text.ilike(f"%{q}%"), Job.employer_name.ilike(f"%{q}%"))
        stmt = stmt.where(kw)
        count_stmt = count_stmt.where(kw)

    if country:
        stmt = stmt.where(Job.location_country == country.upper())
        count_stmt = count_stmt.where(Job.location_country == country.upper())
    if city:
        stmt = stmt.where(Job.location_city.ilike(f"%{city}%"))
        count_stmt = count_stmt.where(Job.location_city.ilike(f"%{city}%"))
    if remote:
        stmt = stmt.where(Job.is_remote == True)
        count_stmt = count_stmt.where(Job.is_remote == True)
    if employment_type:
        stmt = stmt.where(Job.employment_type == employment_type)
        count_stmt = count_stmt.where(Job.employment_type == employment_type)
    if seniority:
        stmt = stmt.where(Job.seniority == seniority)
        count_stmt = count_stmt.where(Job.seniority == seniority)
    if employer:
        emp_f = or_(Job.employer_domain.ilike(f"%{employer}%"), Job.employer_name.ilike(f"%{employer}%"))
        stmt = stmt.where(emp_f)
        count_stmt = count_stmt.where(emp_f)
    # Coerce salary string params to int, ignoring blanks / non-numeric
    def _to_int(s):
        try:
            return int(s) if s and str(s).strip() else None
        except (TypeError, ValueError):
            return None
    salary_min_n = _to_int(salary_min)
    salary_max_n = _to_int(salary_max)
    if salary_min_n:
        stmt = stmt.where(Job.salary_max >= salary_min_n)
        count_stmt = count_stmt.where(Job.salary_max >= salary_min_n)
    if salary_max_n:
        stmt = stmt.where(Job.salary_min <= salary_max_n)
        count_stmt = count_stmt.where(Job.salary_min <= salary_max_n)

    if posted_within:
        days_map = {"1d": 1, "7d": 7, "30d": 30}
        days = days_map.get(posted_within)
        if days:
            cutoff = datetime.utcnow() - timedelta(days=days)
            stmt = stmt.where(Job.date_posted >= cutoff)
            count_stmt = count_stmt.where(Job.date_posted >= cutoff)

    if sort == "salary_desc":
        stmt = stmt.order_by(Job.salary_max.desc().nullslast())
    else:
        stmt = stmt.order_by(Job.date_posted.desc().nullslast())

    total = (await session.execute(count_stmt)).scalar() or 0
    total_pages = max(1, math.ceil(total / per_page))
    offset = (page - 1) * per_page
    stmt = stmt.offset(offset).limit(per_page)

    result = await session.execute(stmt)
    jobs = [_job_to_template_obj(j) for j in result.scalars().all()]

    # Build query string for pagination (excluding page)
    params = {k: v for k, v in request.query_params.items() if k != "page" and v}
    query_string = "&".join(f"{k}={v}" for k, v in params.items())

    filters = {
        "country": country,
        "city": city,
        "remote": remote,
        "employment_type": employment_type,
        "seniority": seniority,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "posted_within": posted_within,
    }

    # Empty-state suggestions: top employers / categories when no results
    suggestions = []
    if total == 0 and q:
        # Find similar titles by trigram-ish ilike on word fragments
        words = [w for w in q.split() if len(w) >= 4]
        if words:
            sug_kw = or_(*[Job.title.ilike(f"%{w}%") for w in words])
            sug_result = await session.execute(
                select(Job.title, Job.id, Job.employer_name)
                .where(Job.status == "active")
                .where(sug_kw)
                .limit(5)
            )
            suggestions = [
                {"title": r[0], "id": str(r[1]), "employer_name": r[2]}
                for r in sug_result.all()
            ]

    return templates.TemplateResponse(request, "search.html", {
        "jobs": jobs,
        "q": q,
        "filters": filters,
        "meta": {"total": total, "page": page, "per_page": per_page, "total_pages": total_pages},
        "query_string": query_string,
        "suggestions": suggestions,
    })


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail_page(
    request: Request,
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    try:
        uid = UUID(job_id)
    except ValueError:
        # Not a UUID — delegate to the long-tail landing handler so URLs
        # like /jobs/sales-engineer-in-sydney render the role+city page.
        from src.api.landing import role_in_city_landing
        return await role_in_city_landing(request, job_id, session)

    result = await session.execute(select(Job).where(Job.id == uid))
    job = result.scalar_one_or_none()

    if not job:
        return HTMLResponse("<h1>Job not found</h1>", status_code=404)

    # Expired / removed postings must not be served as live, indexable pages.
    # Google's job-posting guidance: once a job is gone, return 404/410 (or
    # noindex) — otherwise the stale page is reclassified "crawled – currently
    # not indexed" and erodes crawl trust for the whole sitemap. We return 410
    # with a noindex "this job has closed" page that links back to live jobs.
    if _is_expired_job(job):
        return await _render_expired_job(request, job, session)

    job_obj = _job_to_template_obj(job)

    # Similar jobs — same employer first; if not enough, top up with same primary category
    similar_stmt = (
        select(Job)
        .where(Job.status == "active")
        .where(Job.id != job.id)
        .where(Job.employer_id == job.employer_id)
        .order_by(Job.date_posted.desc().nullslast())
        .limit(5)
    )
    similar_rows = (await session.execute(similar_stmt)).scalars().all()
    if len(similar_rows) < 5 and job.categories:
        primary_cat = job.categories[0]
        topup_stmt = (
            select(Job)
            .where(Job.status == "active")
            .where(Job.id != job.id)
            .where(Job.categories.any(primary_cat))
            .order_by(Job.date_posted.desc().nullslast())
            .limit(5 - len(similar_rows))
        )
        topup_rows = (await session.execute(topup_stmt)).scalars().all()
        existing_ids = {r.id for r in similar_rows}
        for r in topup_rows:
            if r.id not in existing_ids:
                similar_rows.append(r)
                if len(similar_rows) >= 5:
                    break
    similar_jobs = [_job_to_template_obj(r) for r in similar_rows[:5]]

    # Employer panel
    employer_panel = None
    emp: Employer | None = None
    if job.employer_id:
        emp_result = await session.execute(select(Employer).where(Employer.id == job.employer_id))
        emp = emp_result.scalar_one_or_none()
        if emp:
            emp_jobs = (await session.execute(
                select(func.count()).select_from(Job)
                .where(Job.status == "active")
                .where(Job.employer_id == emp.id)
            )).scalar() or 0
            employer_panel = {
                "id": str(emp.id),
                "name": emp.name,
                "domain": emp.domain,
                "logo_url": emp.logo_url,
                "ats_platform": emp.ats_platform,
                "career_page_url": emp.career_page_url,
                "country": emp.country,
                "job_count": emp_jobs,
                "initial": (emp.name[:1] or "?").upper(),
            }

    json_ld = _build_json_ld(job, emp)
    faqs = _build_faqs(job, emp)
    canonical = _canonical_url(f"/jobs/{job.id}")

    return templates.TemplateResponse(request, "job_detail.html", {
        "job": job_obj,
        "json_ld": json_ld,
        "faqs": faqs,
        "canonical_url": canonical,
        "similar_jobs": similar_jobs,
        "employer_panel": employer_panel,
    })


async def _render_expired_job(request: Request, job: Job, session: AsyncSession):
    """Render the 'this job has closed' page for an expired posting: HTTP 410
    (Gone), noindex (no JobPosting JSON-LD), and a handful of live jobs in the
    same category so the visitor — and crawlers — have somewhere live to go."""
    similar_rows: list[Job] = []
    if job.categories:
        similar_rows = (await session.execute(
            select(Job)
            .where(Job.status == "active")
            .where(Job.id != job.id)
            .where(Job.categories.any(job.categories[0]))
            .order_by(Job.date_posted.desc().nullslast())
            .limit(6)
        )).scalars().all()
    similar_jobs = [_job_to_template_obj(r) for r in similar_rows]
    return templates.TemplateResponse(
        request,
        "job_expired.html",
        {
            "job": _job_to_template_obj(job),
            "canonical_url": _canonical_url(f"/jobs/{job.id}"),
            "similar_jobs": similar_jobs,
        },
        status_code=410,
    )


@router.get("/for-ai", response_class=HTMLResponse)
async def for_ai_page(request: Request, session: AsyncSession = Depends(get_session)):
    """Integration paths for AI assistants (ChatGPT, Claude, MCP) and AI labs."""
    active_result = await session.execute(
        select(func.count()).select_from(Job).where(Job.status == "active")
    )
    employer_result = await session.execute(select(func.count()).select_from(Employer))
    countries_result = await session.execute(
        select(func.count(func.distinct(Job.location_country)))
        .where(Job.status == "active")
        .where(Job.location_country.is_not(None))
    )
    stats = {
        "active_jobs": active_result.scalar() or 0,
        "total_employers": employer_result.scalar() or 0,
        "countries": countries_result.scalar() or 0,
    }
    base = str(request.base_url).rstrip("/")
    return templates.TemplateResponse(request, "for_ai.html", {
        "stats": stats,
        "base": base,
    })


@router.get("/employers", response_class=HTMLResponse)
async def employers_page(
    request: Request,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = 30,
    session: AsyncSession = Depends(get_session),
):
    job_count_subq = (
        select(Job.employer_id, func.count(Job.id).label("job_count"))
        .where(Job.status == "active")
        .group_by(Job.employer_id)
        .subquery()
    )

    stmt = (
        select(Employer, func.coalesce(job_count_subq.c.job_count, 0).label("job_count"))
        .outerjoin(job_count_subq, Employer.id == job_count_subq.c.employer_id)
    )
    count_stmt = select(func.count()).select_from(Employer)

    if q:
        qf = or_(Employer.name.ilike(f"%{q}%"), Employer.domain.ilike(f"%{q}%"))
        stmt = stmt.where(qf)
        count_stmt = count_stmt.where(qf)

    stmt = stmt.order_by(func.coalesce(job_count_subq.c.job_count, 0).desc())

    total = (await session.execute(count_stmt)).scalar() or 0
    total_pages = max(1, math.ceil(total / per_page))
    offset = (page - 1) * per_page
    stmt = stmt.offset(offset).limit(per_page)

    result = await session.execute(stmt)
    rows = result.all()

    ids = [row[0].id for row in rows]
    slugs_by_id: dict[str, str] = {}
    if ids:
        try:
            slug_result = await session.execute(
                text("SELECT id::text, slug FROM employers WHERE id = ANY(:ids) AND slug IS NOT NULL"),
                {"ids": [str(i) for i in ids]},
            )
            slugs_by_id = {r[0]: r[1] for r in slug_result.all()}
        except Exception as e:
            logger.warning("slug fetch failed (column may be missing): %s", e)

    employers = [
        {
            "id": str(row[0].id),
            "slug": slugs_by_id.get(str(row[0].id)) or str(row[0].id),
            "name": row[0].name,
            "domain": row[0].domain,
            "logo_url": row[0].logo_url,
            "ats_platform": row[0].ats_platform,
            "career_page_url": row[0].career_page_url,
            "country": row[0].country,
            "job_count": row[1],
        }
        for row in rows
    ]

    return templates.TemplateResponse(request, "employers.html", {
        "employers": employers,
        "q": q,
        "meta": {"total": total, "page": page, "per_page": per_page, "total_pages": total_pages},
    })


@router.get("/employers/{slug}", response_class=HTMLResponse)
async def employer_detail(
    request: Request,
    slug: str,
    page: int = Query(1, ge=1),
    per_page: int = 20,
    session: AsyncSession = Depends(get_session),
):
    """Single-employer page: company info + their active job listings.

    Accepts either the human-readable slug (preferred) or a legacy UUID id.
    Legacy UUID URLs 301-redirect to the canonical slug URL when a slug
    is available. Slug lookups are wrapped in try/except so the route
    keeps working even if the slug column hasn't been migrated yet.
    """
    employer = None
    uid = None
    try:
        uid = UUID(slug)
    except ValueError:
        pass

    if uid is not None:
        emp_result = await session.execute(select(Employer).where(Employer.id == uid))
        employer = emp_result.scalar_one_or_none()
        if employer:
            canonical_slug = None
            try:
                canonical = await session.execute(
                    text("SELECT slug FROM employers WHERE id = :id"), {"id": uid}
                )
                canonical_slug = canonical.scalar_one_or_none()
            except Exception as e:
                logger.warning("slug lookup failed: %s", e)
            if canonical_slug and canonical_slug != slug:
                return RedirectResponse(f"/employers/{canonical_slug}", status_code=301)
    else:
        try:
            id_result = await session.execute(
                text("SELECT id FROM employers WHERE slug = :slug LIMIT 1"), {"slug": slug}
            )
            slug_id = id_result.scalar_one_or_none()
            if slug_id:
                emp_result = await session.execute(select(Employer).where(Employer.id == slug_id))
                employer = emp_result.scalar_one_or_none()
                uid = slug_id
        except Exception as e:
            logger.warning("slug-to-id lookup failed: %s", e)

    if not employer:
        return HTMLResponse("<h1>Employer not found</h1>", status_code=404)

    # Active jobs at this employer
    count_stmt = (
        select(func.count())
        .select_from(Job)
        .where(Job.status == "active")
        .where(Job.employer_id == uid)
    )
    total = (await session.execute(count_stmt)).scalar() or 0
    total_pages = max(1, math.ceil(total / per_page))
    offset = (page - 1) * per_page

    jobs_stmt = (
        select(Job)
        .where(Job.status == "active")
        .where(Job.employer_id == uid)
        .order_by(Job.date_posted.desc().nullslast())
        .offset(offset)
        .limit(per_page)
    )
    jobs_rows = (await session.execute(jobs_stmt)).scalars().all()
    jobs = [_job_to_template_obj(j) for j in jobs_rows]

    # Top countries + categories for this employer
    countries_result = await session.execute(
        select(Job.location_country, func.count(Job.id))
        .where(Job.status == "active")
        .where(Job.employer_id == uid)
        .where(Job.location_country.isnot(None))
        .group_by(Job.location_country)
        .order_by(func.count(Job.id).desc())
        .limit(8)
    )
    countries = [{"code": c, "name": _COUNTRY_NAMES.get(c, c), "count": n} for c, n in countries_result.all()]

    # Top cities served by this employer (for "Locations served" block).
    cities_result = await session.execute(
        select(Job.location_city, Job.location_country, func.count(Job.id))
        .where(Job.status == "active")
        .where(Job.employer_id == uid)
        .where(Job.location_city.isnot(None))
        .where(Job.location_city != "")
        .group_by(Job.location_city, Job.location_country)
        .order_by(func.count(Job.id).desc())
        .limit(12)
    )
    cities = [
        {"name": row[0], "country": row[1], "count": row[2]}
        for row in cities_result.all() if row[0]
    ]

    # Popular job categories (industry specialisms). categories is a
    # Postgres TEXT[] — unnest it and count.
    cat_rows = await session.execute(
        text(
            "SELECT unnest(categories) AS cat, COUNT(*) AS n "
            "FROM jobs "
            "WHERE employer_id = :uid AND status = 'active' AND categories IS NOT NULL "
            "GROUP BY cat ORDER BY n DESC LIMIT 12"
        ),
        {"uid": uid},
    )
    categories_list = [
        {"name": row[0], "count": row[1]}
        for row in cat_rows.all() if row[0]
    ]

    # Synthesize a description if the employer record doesn't have one
    # (the Employer model has no description column today). Built from
    # the live data so it reflects current openings rather than stale copy.
    bits: list[str] = [f"{employer.name} is hiring."]
    if total:
        bits.append(f"There are {total:,} active openings.")
    if categories_list:
        top_cats = ", ".join(c["name"] for c in categories_list[:4])
        bits.append(f"Top specialisms: {top_cats}.")
    if countries:
        top_countries = ", ".join(c["name"] for c in countries[:4])
        bits.append(f"Hiring across {top_countries}.")
    if cities:
        top_cities = ", ".join(c["name"] for c in cities[:5])
        bits.append(f"Active in {top_cities}.")
    description = " ".join(bits)

    website = f"https://{employer.domain}" if employer.domain else None

    faqs: list[dict] = []
    faqs.append({
        "q": f"How many jobs does {employer.name} have open?",
        "a": f"{employer.name} currently has {total:,} active job listing{'s' if total != 1 else ''} indexed by ZammeJobs.",
    })
    if cities:
        faqs.append({
            "q": f"Where does {employer.name} hire?",
            "a": (
                f"Active hiring locations include "
                f"{', '.join(c['name'] for c in cities[:5])}."
            ),
        })
    if categories_list:
        faqs.append({
            "q": f"What roles does {employer.name} hire for?",
            "a": (
                "Recent openings span "
                f"{', '.join(c['name'] for c in categories_list[:5])}."
            ),
        })
    if website:
        faqs.append({
            "q": f"Where do I apply for {employer.name} jobs?",
            "a": (
                f"Click Apply on any listing on this page — you'll go straight to the "
                f"role on {employer.name}'s own site ({website}). ZammeJobs never inserts a "
                "middleman form."
            ),
        })

    employer_obj = {
        "id": str(employer.id),
        "name": employer.name,
        "domain": employer.domain,
        "website": website,
        "logo_url": employer.logo_url,
        "ats_platform": employer.ats_platform,
        "career_page_url": employer.career_page_url,
        "country": employer.country,
        "country_name": _COUNTRY_NAMES.get(employer.country or "", employer.country or ""),
        "initial": (employer.name[:1] or "?").upper(),
        "active_job_count": total,
        "description": description,
        "claimed": employer.claimed,
    }

    canonical = _canonical_url(f"/employers/{slug}")
    org_schema = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "@id": f"{canonical}#org",
        "name": employer.name,
        "url": website or canonical,
        "sameAs": [website] if website else [],
        "description": description,
    }
    if employer.logo_url:
        org_schema["logo"] = employer.logo_url
    if employer.country:
        org_schema["address"] = {
            "@type": "PostalAddress",
            "addressCountry": employer.country,
        }

    breadcrumb_schema = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Jobs", "item": _canonical_url("/")},
            {"@type": "ListItem", "position": 2, "name": "Employers", "item": _canonical_url("/employers")},
            {"@type": "ListItem", "position": 3, "name": employer.name, "item": canonical},
        ],
    }

    faq_schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": f["q"],
                "acceptedAnswer": {"@type": "Answer", "text": f["a"]},
            }
            for f in faqs
        ],
    }

    return templates.TemplateResponse(request, "employer_detail.html", {
        "employer": employer_obj,
        "jobs": jobs,
        "countries": countries,
        "cities": cities,
        "categories": categories_list,
        "faqs": faqs,
        "org_schema": json.dumps(org_schema, ensure_ascii=False),
        "breadcrumb_schema": json.dumps(breadcrumb_schema, ensure_ascii=False),
        "faq_schema": json.dumps(faq_schema, ensure_ascii=False),
        "meta": {"total": total, "page": page, "per_page": per_page, "total_pages": total_pages},
    })


@router.get("/apply/{job_id}")
async def apply_redirect(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Redirect to the job's apply URL on the employer/tenant site, with
    UTM appended. We do NOT pre-check liveness — many ATS sites (and
    Shazamme tenants behind Cloudflare) block HEAD/bot user-agents, so
    the check was falsely classifying live jobs as dead and bouncing
    users to the employer homepage. Shazamme manages job lifecycle via
    the daily feed; expired rows drop out on next ingest."""
    try:
        uid = UUID(job_id)
    except ValueError:
        return RedirectResponse("/", status_code=302)

    result = await session.execute(select(Job).where(Job.id == uid))
    job = result.scalar_one_or_none()
    if not job:
        return RedirectResponse("/", status_code=302)

    if job.source_url:
        return RedirectResponse(_append_utm(job.source_url), status_code=302)

    employer = None
    if job.employer_id:
        emp_result = await session.execute(select(Employer).where(Employer.id == job.employer_id))
        employer = emp_result.scalar_one_or_none()

    # source_url missing — last resort, send to the employer's career page.
    # No DB mutation here; if the URL is genuinely empty in the feed, the
    # next Shazamme import will refresh or drop the row.
    fallback = _employer_fallback_url(job, employer)
    return RedirectResponse(_append_utm(fallback), status_code=302)
