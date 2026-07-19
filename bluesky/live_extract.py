"""
bluesky_worldcup_collector.py

Live collector for Bluesky posts related to the FIFA World Cup, using
Bluesky's Jetstream firehose (a lightweight JSON view of the full AT
Protocol firehose -- not a sampled stream, it's the complete public feed).

Designed to run continuously during matches:
  - Automatically reconnects on dropped connections (websocket errors,
    network blips, server restarts) using exponential backoff.
  - Resumes from a saved cursor (last processed timestamp) instead of
    starting over, so a brief outage doesn't create a data gap.
  - Buffers matched posts and flushes them to disk regularly (by count
    AND by time), so a crash never loses more than a few seconds of data.
  - Resolves author DIDs to human-readable handles (usernames), with
    caching so the same author isn't looked up repeatedly.

Requirements:
    pip install websockets aiohttp --break-system-packages

Run:
    python bluesky_worldcup_collector.py

Stop safely with Ctrl+C -- it flushes any buffered posts before exiting.

Output:
    worldcup_posts_00001.jsonl, _00002.jsonl, ...
                           One JSON object per line, per matched post.
                           A new file starts automatically once the
                           current one reaches MAX_ROWS_PER_FILE rows,
                           so no single file grows unbounded.
    writer_state.json      Tracks current file index/row count (for
                            resuming rotation correctly after a restart).
    worldcup_cursor.txt     Last processed Jetstream cursor (for resuming).
    handle_cache.json       DID -> handle lookup cache.
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
import aiohttp

# ---------------------------------------------------------------------------
# Configuration -- adjust these for your event/language coverage
# ---------------------------------------------------------------------------

# Public Jetstream instances. The script rotates through these on
# reconnect, so an outage on one region doesn't stall collection.
JETSTREAM_ENDPOINTS = [
    "wss://jetstream1.us-east.bsky.network/subscribe",
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
    "wss://jetstream2.us-west.bsky.network/subscribe",
]

# Keep this broad -- people tag inconsistently, drop hashtags, misspell,
# or post in other languages. Add team names, player names, or match
# hashtags (e.g. "#ARGFRA") as your event approaches.
KEYWORDS = [
    "world cup", "worldcup", "fifa world cup", "fifaworldcup",
    "#worldcup", "#fifaworldcup", "#fifa", "#wc2026",
    # Spanish / Portuguese / French -- broaden as needed for your event
    # "mundial", "copa del mundo", "coupe du monde",
]

OUTPUT_DIR = Path("bs_fifa_final")
OUTPUT_PREFIX = "worldcup_posts"   # files will be named worldcup_posts_00001.jsonl, _00002.jsonl, ...
CURSOR_FILE = Path("worldcup_cursor_final.txt")
HANDLE_CACHE_FILE = Path("handle_cache_final.json")

MAX_ROWS_PER_FILE = 2000    # start a new file once the current one reaches this many rows
SAVE_EVERY_N_POSTS = 20     # flush to disk after this many matched posts
SAVE_EVERY_SECONDS = 15     # ...or after this many seconds, whichever first
RECONNECT_BASE_DELAY = 2    # seconds -- exponential backoff starting point
RECONNECT_MAX_DELAY = 60    # cap on backoff delay
HANDLE_RESOLVE_CONCURRENCY = 5   # simultaneous handle lookups

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("worldcup_collector")


# ---------------------------------------------------------------------------
# Cursor + cache persistence helpers
# ---------------------------------------------------------------------------

def load_cursor():
    """Load the last processed Jetstream timestamp, if any, so a restart
    resumes close to where collection left off instead of from scratch."""
    if CURSOR_FILE.exists():
        try:
            return int(CURSOR_FILE.read_text().strip())
        except ValueError:
            return None
    return None


def save_cursor(time_us: int):
    CURSOR_FILE.write_text(str(time_us))


def load_handle_cache() -> dict:
    if HANDLE_CACHE_FILE.exists():
        try:
            return json.loads(HANDLE_CACHE_FILE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def save_handle_cache(cache: dict):
    HANDLE_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False))


def matches_keywords(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(kw in lowered for kw in KEYWORDS)


# ---------------------------------------------------------------------------
# DID -> handle (username) resolution, cached and rate-limited
# ---------------------------------------------------------------------------

class HandleResolver:
    """Bluesky's firehose only carries the author's DID (a stable but
    unreadable identifier), not their @handle. This resolves DIDs to
    handles via the public API, caching results so repeat posters from
    the same account don't trigger repeat lookups."""

    def __init__(self):
        self.cache = load_handle_cache()
        self.sem = asyncio.Semaphore(HANDLE_RESOLVE_CONCURRENCY)
        self._session = None
        self._dirty = False
        self.rate_limit_hits = 0

    async def start(self):
        self._session = aiohttp.ClientSession()

    async def stop(self):
        if self._session:
            await self._session.close()
        if self._dirty:
            save_handle_cache(self.cache)

    async def resolve(self, did: str) -> str:
        if did in self.cache:
            return self.cache[did]
        async with self.sem:
            handle = did  # fall back to the DID itself if lookup fails
            try:
                url = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"
                async with self._session.get(url, params={"actor": did}, timeout=10) as resp:
                    if resp.status == 429:
                        # Rate limited -- log it loudly (not silently) and
                        # back off before giving up on this lookup, so
                        # throttling shows up in your logs instead of just
                        # quietly degrading to raw DIDs in the output.
                        retry_after = resp.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after else 5.0
                        self.rate_limit_hits += 1
                        log.warning(
                            f"Rate limited on handle resolution (429). "
                            f"Total rate-limit hits so far: {self.rate_limit_hits}. "
                            f"Backing off {wait:.0f}s before continuing."
                        )
                        await asyncio.sleep(wait)
                    elif resp.status == 200:
                        data = await resp.json()
                        handle = data.get("handle", did)
                    else:
                        log.debug(f"getProfile returned status {resp.status} for {did}")
            except Exception as e:
                log.debug(f"Handle resolve failed for {did}: {e}")
        self.cache[did] = handle
        self._dirty = True
        return handle


# ---------------------------------------------------------------------------
# Buffered, periodic-flush writer -- protects against losing data on crash
# ---------------------------------------------------------------------------

class BufferedWriter:
    def __init__(self, output_dir: Path, prefix: str, max_rows_per_file: int):
        self.output_dir = output_dir
        self.prefix = prefix
        self.max_rows_per_file = max_rows_per_file
        self.buffer = []
        self.last_flush = time.time()

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Resume file index/row count from disk, so restarting the script
        # continues rotation correctly instead of overwriting file 1.
        state = self._load_state()
        self.file_index = state.get("file_index", 1)
        self.rows_in_current_file = state.get("rows_in_current_file", 0)

    def _state_path(self) -> Path:
        return self.output_dir / "writer_state.json"

    def _load_state(self) -> dict:
        p = self._state_path()
        if p.exists():
            try:
                return json.loads(p.read_text())
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_state(self):
        self._state_path().write_text(json.dumps({
            "file_index": self.file_index,
            "rows_in_current_file": self.rows_in_current_file,
        }))

    def _current_path(self) -> Path:
        return self.output_dir / f"{self.prefix}_{self.file_index:05d}.jsonl"

    def add(self, record: dict):
        self.buffer.append(record)

    def should_flush(self) -> bool:
        return (
            len(self.buffer) >= SAVE_EVERY_N_POSTS
            or (time.time() - self.last_flush) >= SAVE_EVERY_SECONDS
        )

    def flush(self):
        if not self.buffer:
            return

        remaining = self.buffer
        while remaining:
            space_left = self.max_rows_per_file - self.rows_in_current_file
            chunk, remaining = remaining[:space_left], remaining[space_left:]

            if chunk:
                path = self._current_path()
                with open(path, "a", encoding="utf-8") as f:
                    for rec in chunk:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                self.rows_in_current_file += len(chunk)
                log.info(f"Flushed {len(chunk)} post(s) to {path} "
                         f"({self.rows_in_current_file}/{self.max_rows_per_file} rows)")

            if self.rows_in_current_file >= self.max_rows_per_file:
                log.info(f"{self._current_path()} reached {self.max_rows_per_file} rows -- "
                         f"rotating to a new file.")
                self.file_index += 1
                self.rows_in_current_file = 0

            self._save_state()

        self.buffer.clear()
        self.last_flush = time.time()


# ---------------------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------------------

async def collect(endpoints=JETSTREAM_ENDPOINTS):
    writer = BufferedWriter(OUTPUT_DIR, OUTPUT_PREFIX, MAX_ROWS_PER_FILE)
    resolver = HandleResolver()
    await resolver.start()

    stop_event = asyncio.Event()

    def _handle_signal():
        log.info("Shutdown signal received -- flushing buffer and exiting...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # signal handlers aren't supported on some platforms (e.g. Windows)

    endpoint_idx = 0
    attempt = 0
    matched_count = 0
    seen_count = 0

    try:
        while not stop_event.is_set():
            endpoint = endpoints[endpoint_idx % len(endpoints)]
            cursor = load_cursor()
            params = "wantedCollections=app.bsky.feed.post"
            if cursor:
                params += f"&cursor={cursor}"
            url = f"{endpoint}?{params}"

            try:
                log.info(f"Connecting to {endpoint} (resuming from cursor={cursor})")
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, max_size=None
                ) as ws:
                    attempt = 0  # reset backoff after a clean connect

                    async for message in ws:
                        if stop_event.is_set():
                            break

                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        # Persist the cursor continuously (not just on
                        # matches) so a resume after any drop is accurate.
                        time_us = data.get("time_us")
                        if time_us:
                            save_cursor(time_us)

                        if data.get("kind") != "commit":
                            continue
                        commit = data.get("commit", {})
                        if commit.get("collection") != "app.bsky.feed.post":
                            continue
                        if commit.get("operation") != "create":
                            continue

                        record = commit.get("record", {})
                        text = record.get("text", "")
                        seen_count += 1

                        if not matches_keywords(text):
                            if writer.should_flush():
                                writer.flush()
                            continue

                        did = data.get("did", "")
                        rkey = commit.get("rkey", "")
                        handle = await resolver.resolve(did)
                        reply = record.get("reply") or {}

                        post_record = {
                            "id": f"{did}/{rkey}",
                            "uri": f"at://{did}/app.bsky.feed.post/{rkey}",
                            "cid": commit.get("cid"),
                            "did": did,
                            "username": handle,
                            "created_at": record.get("createdAt"),
                            "text": text,
                            "langs": record.get("langs", []),
                            "reply_root_uri": (reply.get("root") or {}).get("uri"),
                            "reply_parent_uri": (reply.get("parent") or {}).get("uri"),
                            "collected_at_utc": datetime.now(timezone.utc).isoformat(),
                        }

                        writer.add(post_record)
                        matched_count += 1

                        if matched_count % 50 == 0:
                            log.info(
                                f"Matched {matched_count} posts so far "
                                f"({seen_count} total posts scanned)"
                            )

                        if writer.should_flush():
                            writer.flush()

            except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"Connection lost ({e!r}). Will reconnect...")
            except Exception as e:
                # Catch-all: log and keep the process alive rather than
                # letting one bad message or transient error kill the run.
                log.exception(f"Unexpected error in collection loop: {e}")
            finally:
                writer.flush()  # always persist buffered posts before reconnecting

            if stop_event.is_set():
                break

            attempt += 1
            delay = min(RECONNECT_BASE_DELAY * (2 ** (attempt - 1)), RECONNECT_MAX_DELAY)
            endpoint_idx += 1  # try a different Jetstream instance next time
            log.info(f"Reconnecting in {delay:.0f}s (attempt {attempt})...")
            await asyncio.sleep(delay)

    finally:
        writer.flush()
        await resolver.stop()
        log.info(
            f"Shutdown complete. Total matched posts: {matched_count}, "
            f"total posts scanned: {seen_count}, "
            f"handle-resolution rate-limit hits: {resolver.rate_limit_hits}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(collect())
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
