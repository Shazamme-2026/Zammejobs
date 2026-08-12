"""Sitemap.xml, robots.txt, and AI well-known endpoints for AI/SEO discovery."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import get_session
from src.models import Employer, Job

router = APIRouter(tags=["SEO"])


# Major AI training and live-search crawlers. We allow every one explicitly:
# silence in robots.txt is interpreted by some of them (Google-Extended,
# Applebot-Extended, anthropic-ai) as "default deny", so listing each is the
# only way to be sure they index us.
AI_ALLOWLIST = [
    # Anthropic
    "ClaudeBot", "ClaudeUser", "ClaudeWeb", "Claude-Web", "anthropic-ai",
    # OpenAI
    "GPTBot", "OAI-SearchBot", "ChatGPT-User", "ChatGPTBot",
    # Google AI / Search
    "Googlebot", "Googlebot-Image", "Googlebot-News", "Google-Extended", "Storebot-Google",
    # Microsoft / Bing
    "Bingbot", "BingPreview", "MSNBot", "msnbot-media",
    # Apple
    "Applebot", "Applebot-Extended",
    # Perplexity
    "PerplexityBot", "Perplexity-User",
    # Common Crawl (foundation of many LLM training datasets)
    "CCBot",
    # Meta
    "FacebookBot", "facebookexternalhit", "meta-externalagent", "meta-externalfetcher",
    # Other engines / agents
    "DuckDuckBot", "YandexBot", "Baiduspider",
    "Diffbot", "Bytespider", "PetalBot",
    "Cohere-AI", "cohere-ai",
    "Mistral-AI",
    "YouBot",
    "Amazonbot",
    "Timpibot",
    "ImagesiftBot",
]


@router.get("/robots.txt", response_class=PlainTextResponse)
async def robots_txt(request: Request):
    base = "https://www.zammejobs.com"
    blocks = [f"# ZammeJobs — every job, AI-indexable. Crawl us!"]
    for bot in AI_ALLOWLIST:
        blocks.append(f"\nUser-agent: {bot}\nAllow: /\nCrawl-delay: 0")
    blocks.append("\nUser-agent: *\nAllow: /")
    blocks.append(f"\nSitemap: {base}/sitemap.xml")
    blocks.append(f"Sitemap: {base}/sitemap-jobs.xml")
    blocks.append(f"Sitemap: {base}/sitemap-employers.xml")
    return "\n".join(blocks) + "\n"


@router.get("/humans.txt", response_class=PlainTextResponse, summary="humans.txt — credits + contact")
async def humans_txt(request: Request):
    base = "https://www.zammejobs.com"
    return f"""/* TEAM */
ZammeJobs — global AI-native job index.
Site:    {base}
Contact: hello@zammejobs.com
Twitter: @zammejobs

/* SITE */
Last update:  2026
Standards:    HTML5, schema.org JSON-LD, OpenAPI 3.1, MCP, sitemap.org
Components:   FastAPI, PostgreSQL, Alembic, Jinja2, Inter
License:      CC-BY-4.0 (data) — code under company licence

/* MISSION */
Make every job on the public web visible to AI search. Crawl the canonical
ATS source, normalize to JSON-LD, publish free under CC-BY-4.0. Hire with
AI, get hired with AI — no paywalls, no lock-in.

/* FOR HUMANS */
Found a bug, want to register your career site, or want a partnership?
Email hello@zammejobs.com. We answer.
"""


@router.get("/ai.txt", response_class=PlainTextResponse, summary="ai.txt — AI usage policy")
async def ai_txt(request: Request):
    base = "https://www.zammejobs.com"
    return f"""# ai.txt — ZammeJobs AI usage policy
# {base}
# Spec inspiration: https://site.spawning.ai/spawning-ai-txt

# ───────────────────────────────────────────────────────────────
# Summary
# ───────────────────────────────────────────────────────────────
# All public job-index data on this site is licensed CC-BY-4.0.
# AI training, RAG retrieval, agent calls, and embedding into
# foundation models are EXPLICITLY ALLOWED. Attribution is
# preferred but NOT REQUIRED. There is no rate limit on the
# public REST API or JSONL feed for AI assistants and AI labs.

User-agent: *
Allow: /
Allow: /api/
Allow: /data/
Allow: /jobs/
Allow: /employers/
Allow: /sitemap.xml
Allow: /sitemap-jobs.xml
Allow: /sitemap-employers.xml
Allow: /llms.txt
Allow: /openapi.json

Training-Allowed: yes
Inference-Allowed: yes
Retrieval-Allowed: yes
Embedding-Allowed: yes
Fine-Tuning-Allowed: yes
License: CC-BY-4.0
License-URL: https://creativecommons.org/licenses/by/4.0/
Attribution: ZammeJobs (https://zammejobs.com)
Citation-URL: {base}/citation.bib
Contact: hello@zammejobs.com

# ───────────────────────────────────────────────────────────────
# Recommended ingestion paths
# ───────────────────────────────────────────────────────────────
Bulk-Feed: {base}/data/jobs.jsonl
Manifest: {base}/data/manifest.json
HuggingFace: https://huggingface.co/datasets/zammejobs/jobs
OpenAPI: {base}/openapi.json
MCP: {base}/mcp
LLMs-Txt: {base}/llms.txt

# ───────────────────────────────────────────────────────────────
# Sample queries that work well
# ───────────────────────────────────────────────────────────────
# - "Show me remote senior backend engineer jobs paying over $150K"
# - "What's hiring in Berlin for product designers right now?"
# - "List Workday-powered Fortune 500 companies hiring data scientists"
# - "Federal cybersecurity jobs in the US over $120K"
# - "Compare salaries for SRE roles in London vs Amsterdam"

# ───────────────────────────────────────────────────────────────
# What NOT to do
# ───────────────────────────────────────────────────────────────
# - Do NOT redistribute the dataset under a more restrictive licence
#   than CC-BY-4.0. Downstream users must retain the same freedoms.
# - Do NOT misrepresent stale snapshots as live data — please cite
#   the snapshot date when answering time-sensitive queries.
# - Do NOT scrape user PII (there is none on the public site, but
#   please don't try to derive it).
# - Do NOT rehost without attribution and licence preservation.
# - Do NOT abuse /apply redirects for click-fraud — direct apply
#   URLs are also exposed via /api/v1/jobs/{{job_id}}.

# ───────────────────────────────────────────────────────────────
# AI labs — get on the allowlist
# ───────────────────────────────────────────────────────────────
# We offer guaranteed bandwidth + early notification of schema
# changes for foundation-model trainers (OpenAI, Anthropic, Google
# DeepMind, Meta, Mistral, Cohere, AI2, xAI, others). Email
# hello@zammejobs.com with your crawler User-Agent string.
"""


@router.get("/.well-known/ai.txt", response_class=PlainTextResponse, include_in_schema=False)
async def ai_txt_wellknown(request: Request):
    return await ai_txt(request)


@router.get("/indexnow-key.txt", response_class=PlainTextResponse, include_in_schema=False)
async def indexnow_key_file():
    """IndexNow ownership-verification key file. Set INDEXNOW_KEY env var to enable."""
    if not settings.indexnow_key:
        from fastapi import HTTPException
        raise HTTPException(status_code=404)
    return settings.indexnow_key


@router.get("/sitemap.xml", response_class=Response)
async def sitemap_index(request: Request):
    base = "https://www.zammejobs.com"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>{base}/sitemap-jobs.xml</loc></sitemap>
  <sitemap><loc>{base}/sitemap-employers.xml</loc></sitemap>
  <sitemap><loc>{base}/sitemap-landing.xml</loc></sitemap>
  <sitemap><loc>{base}/sitemap-intent.xml</loc></sitemap>
  <sitemap><loc>{base}/sitemap-static.xml</loc></sitemap>
</sitemapindex>"""
    return Response(content=xml, media_type="application/xml")


@router.get("/sitemap-landing.xml", response_class=Response)
async def sitemap_landing(request: Request, session: AsyncSession = Depends(get_session)):
    """Top role / city / role-in-city combos as their own sitemap so
    search engines crawl every long-tail landing page we generate."""
    from src.api.landing import slugify

    # Force HTTPS on sitemap URLs — Google/Bing dedupe http vs https as
    # distinct URLs, and we're https-only behind Railway's edge.
    base = "https://www.zammejobs.com"
    urls: list[str] = []

    # Junk values the Shazamme feed dumps into <category> that aren't
    # actually job titles — pipeline/workflow status, internal tags.
    JUNK_ROLES = {
        "no", "on-hold", "accepting-candidates", "open", "closed", "draft",
        "active", "inactive", "internal", "external", "various",
        "n-a", "na", "tbd", "tbc", "other", "general", "misc",
    }

    role_rows = (await session.execute(
        select(func.unnest(Job.categories).label("cat"), func.count().label("n"))
        .where(Job.status == "active")
        .group_by("cat")
        .order_by(func.count().desc())
        .limit(300)
    )).all()
    role_slugs = []
    for cat, n in role_rows:
        if not cat or n < 5:
            continue
        s = slugify(cat)
        # Skip junk + sub-3-char slugs ("hr", "it" still pass via 3-char min
        # — these are too ambiguous for the role page to be useful).
        if not s or len(s) < 4 or s in JUNK_ROLES:
            continue
        if s in role_slugs:
            continue
        role_slugs.append(s)
        urls.append(f"{base}/jobs/role/{s}")
        urls.append(f"{base}/salaries/{s}")
        urls.append(f"{base}/skills/{s}")
        if len(role_slugs) >= 150:
            break

    city_rows = (await session.execute(
        select(Job.location_city, func.count().label("n"))
        .where(Job.status == "active", Job.location_city.isnot(None))
        .group_by(Job.location_city)
        .order_by(func.count().desc())
        .limit(300)
    )).all()
    city_slugs = []
    for city, n in city_rows:
        if not city or n < 5:
            continue
        s = slugify(city)
        if not s or len(s) < 3:
            continue
        if s in city_slugs:
            continue
        city_slugs.append(s)
        urls.append(f"{base}/jobs/in/{s}")
        if len(city_slugs) >= 150:
            break

    # Cross-product top-N (capped to avoid explosion).
    for r in role_slugs[:30]:
        for c in city_slugs[:30]:
            urls.append(f"{base}/jobs/{r}-in-{c}")

    body = "\n".join(f"  <url><loc>{u}</loc></url>" for u in urls)
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{body}
</urlset>"""
    return Response(content=xml, media_type="application/xml")


@router.get("/sitemap-intent.xml", response_class=Response)
async def sitemap_intent(request: Request):
    """Curated high-intent landing pages (Nursing in Australia, Mining in WA,
    etc.). Static list so these always make it into the sitemap even if the
    underlying jobs haven't crossed the threshold the auto-landing crawl uses.
    """
    from src.api.landing import CURATED_INTENT_PAGES

    base = "https://www.zammejobs.com"
    urls = [f"{base}/jobs/intent"]
    for entry in CURATED_INTENT_PAGES:
        urls.append(f"{base}/jobs/{entry['slug']}")

    body = "\n".join(
        f"  <url><loc>{u}</loc><changefreq>daily</changefreq><priority>0.8</priority></url>"
        for u in urls
    )
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{body}
</urlset>"""
    return Response(content=xml, media_type="application/xml")


@router.get("/sitemap-jobs.xml", response_class=Response)
async def sitemap_jobs(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    base = "https://www.zammejobs.com"
    # Only live URLs belong in the sitemap. status='active' is the primary gate;
    # the date_expires guard is belt-and-braces so a job carrying an explicit
    # past source expiry is dropped even if the status flip (mark_stale_jobs /
    # feed reconciliation) hasn't caught up yet. A sitemap full of expired URLs
    # destroys Google's trust in it and throttles crawling of the fresh jobs.
    result = await session.execute(
        select(Job.id, Job.date_updated)
        .where(Job.status == "active")
        .where(or_(Job.date_expires.is_(None), Job.date_expires > func.now()))
        .order_by(Job.date_posted.desc().nullslast())
        .limit(50000)
    )
    jobs = result.all()

    urls = []
    for job_id, date_updated in jobs:
        lastmod = date_updated.strftime("%Y-%m-%d") if date_updated else ""
        urls.append(
            f"  <url>\n"
            f"    <loc>{base}/jobs/{job_id}</loc>\n"
            f"    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>daily</changefreq>\n"
            f"    <priority>0.8</priority>\n"
            f"  </url>"
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n</urlset>"
    )
    return Response(content=xml, media_type="application/xml")


@router.get("/sitemap-employers.xml", response_class=Response)
async def sitemap_employers(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    base = "https://www.zammejobs.com"
    try:
        result = await session.execute(
            text("SELECT id::text, slug, updated_at FROM employers ORDER BY updated_at DESC NULLS LAST LIMIT 50000")
        )
        employers = [(r[0], r[1], r[2]) for r in result.all()]
    except Exception:
        # slug column may not exist yet — fall back to id-only URLs
        result = await session.execute(
            select(Employer.id, Employer.updated_at)
            .order_by(Employer.updated_at.desc().nullslast())
            .limit(50000)
        )
        employers = [(str(r[0]), None, r[1]) for r in result.all()]

    urls = [f'  <url><loc>{base}/employers</loc><changefreq>daily</changefreq><priority>0.6</priority></url>']
    for emp_id, slug, updated_at in employers:
        lastmod = updated_at.strftime("%Y-%m-%d") if updated_at else ""
        urls.append(
            f"  <url>\n"
            f"    <loc>{base}/employers/{slug or emp_id}</loc>\n"
            f"    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>weekly</changefreq>\n"
            f"    <priority>0.5</priority>\n"
            f"  </url>"
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n</urlset>"
    )
    return Response(content=xml, media_type="application/xml")


@router.get("/sitemap-static.xml", response_class=Response)
async def sitemap_static(request: Request):
    base = "https://www.zammejobs.com"
    from src.api.industry import INDUSTRIES

    static_routes = [
        ("/", "1.0", "hourly"),
        ("/search", "0.9", "hourly"),
        ("/match", "0.9", "daily"),
        ("/match-resume", "0.9", "daily"),
        ("/alerts", "0.7", "weekly"),
        ("/employers", "0.7", "daily"),
        ("/industry", "0.8", "daily"),
        ("/status", "0.6", "hourly"),
        ("/for-ai", "0.7", "weekly"),
        ("/docs", "0.4", "weekly"),
        ("/llms.txt", "0.5", "daily"),
        ("/ai.txt", "0.4", "weekly"),
        ("/humans.txt", "0.3", "yearly"),
        ("/citation.bib", "0.4", "yearly"),
        ("/jobs.rss", "0.6", "hourly"),
        ("/jobs.atom", "0.6", "hourly"),
        ("/jobs-ai.json", "0.6", "hourly"),
    ]
    # Industry hubs — fixed taxonomy, all worth indexing.
    for slug in INDUSTRIES:
        static_routes.append((f"/industry/{slug}", "0.8", "daily"))
    urls = "\n".join(
        f"  <url><loc>{base}{path}</loc><priority>{prio}</priority><changefreq>{cf}</changefreq></url>"
        for path, prio, cf in static_routes
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + urls
        + "\n</urlset>"
    )
    return Response(content=xml, media_type="application/xml")
