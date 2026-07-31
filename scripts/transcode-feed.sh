#!/usr/bin/env bash
#
# transcode-feed.sh
#
# Vinyl exports some LinkedIn/job XML feeds as UTF-16LE bytes while the XML
# declaration inside the file claims `encoding="utf-8"`. That mismatch makes the
# feed malformed, and strict parsers (e.g. MSEP) correctly reject it. Vinyl's
# export runs on an old SQL Server whose xml/nvarchar output is natively UTF-16,
# so the fix is done downstream: fetch the feed, re-encode the BYTES to real
# UTF-8 (matching the declaration the file already carries), and republish to a
# destination we control with the correct Content-Type.
#
# The re-encode is lossless — content is identical, only the byte encoding
# changes. Nothing in Vinyl is touched. (The same UTF-16LE→UTF-8 fact is handled
# for DB ingestion in src/connectors/shazamme.py; this script republishes for
# external consumers instead.)
#
# Required env:
#   FEED_SRC_URL       Source feed URL (the UTF-16 file)
#   FEED_DEST_BUCKET   Destination S3 bucket
#   FEED_DEST_KEY      Destination S3 key (e.g. feeds/foo.xml)
#
# Optional env:
#   FEED_SRC_ENCODING     iconv source encoding (default: UTF-16LE)
#   FEED_CONTENT_TYPE     (default: application/xml; charset=utf-8)
#   FEED_CACHE_CONTROL    (default: public, max-age=300)
#   FEED_ACL              e.g. public-read (default: unset — rely on bucket policy)
#   FEED_MIN_JOBS         Refuse to publish fewer than N <job> entries (default: 1)
#   FEED_CF_DISTRIBUTION  CloudFront distribution id to invalidate (default: unset)
#
set -euo pipefail

: "${FEED_SRC_URL:?FEED_SRC_URL is required}"
: "${FEED_DEST_BUCKET:?FEED_DEST_BUCKET is required}"
: "${FEED_DEST_KEY:?FEED_DEST_KEY is required}"

SRC_ENCODING="${FEED_SRC_ENCODING:-UTF-16LE}"
CONTENT_TYPE="${FEED_CONTENT_TYPE:-application/xml; charset=utf-8}"
CACHE_CONTROL="${FEED_CACHE_CONTROL:-public, max-age=300}"
MIN_JOBS="${FEED_MIN_JOBS:-1}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
src="$workdir/src.xml"
out="$workdir/out.xml"

echo "→ Fetching source: $FEED_SRC_URL"
curl --fail --silent --show-error --location \
  --retry 5 --retry-delay 10 --retry-all-errors \
  --max-time 300 \
  -H "User-Agent: shazamme-feed-transcode/1.0" \
  -o "$src" \
  "$FEED_SRC_URL"

src_bytes="$(wc -c < "$src" | tr -d ' ')"
echo "→ Source size: ${src_bytes} bytes"
if [ "$src_bytes" -lt 1024 ]; then
  echo "::error::Source feed is suspiciously small (${src_bytes} bytes) — refusing to publish." >&2
  exit 1
fi

# Re-encode bytes to UTF-8. If the source is already valid UTF-8 (e.g. Vinyl gets
# fixed at source one day), fall back to a straight copy so this keeps working.
echo "→ Transcoding ${SRC_ENCODING} → UTF-8"
if iconv -f "$SRC_ENCODING" -t UTF-8 "$src" > "$out" 2>/dev/null && xmllint --noout "$out" 2>/dev/null; then
  echo "  re-encoded from ${SRC_ENCODING}"
elif xmllint --noout "$src" 2>/dev/null; then
  cp "$src" "$out"
  echo "  source was already well-formed (no re-encode needed)"
else
  echo "::error::Could not produce well-formed UTF-8 XML from source." >&2
  exit 1
fi

# Never publish an error page or an empty feed.
job_count="$(grep -c "</job>" "$out" || true)"
echo "→ <job> entries: ${job_count}"
if [ "${job_count:-0}" -lt "$MIN_JOBS" ]; then
  echo "::error::Only ${job_count} job entries (min ${MIN_JOBS}) — refusing to publish." >&2
  exit 1
fi

echo "→ Uploading to s3://${FEED_DEST_BUCKET}/${FEED_DEST_KEY}"
cp_args=(
  "$out" "s3://${FEED_DEST_BUCKET}/${FEED_DEST_KEY}"
  --content-type "$CONTENT_TYPE"
  --cache-control "$CACHE_CONTROL"
)
[ -n "${FEED_ACL:-}" ] && cp_args+=(--acl "$FEED_ACL")
aws s3 cp "${cp_args[@]}"

if [ -n "${FEED_CF_DISTRIBUTION:-}" ]; then
  echo "→ Invalidating CloudFront /${FEED_DEST_KEY}"
  aws cloudfront create-invalidation \
    --distribution-id "$FEED_CF_DISTRIBUTION" \
    --paths "/${FEED_DEST_KEY}" >/dev/null
fi

echo "✓ Published ${job_count} jobs as UTF-8 → ${FEED_DEST_KEY}"
