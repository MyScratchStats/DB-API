"""
Advanced Async Scratch Forum Scraper
- Fetches posts 1 -> MAX_POST_ID
- Writes CSV incrementally
- Writes each post text to posts/{post_id}.txt
- Async + ThreadPoolExecutor for parsing and file I/O
"""

import asyncio
import aiohttp
import csv
import os
import sys
import time
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dateutil import parser as dateparser

# -------------------------
# Configuration
# -------------------------
MAX_POST_ID = 1000
START_POST_ID = 1
CSV_FILE = "scratch_posts.csv"
POSTS_DIR = "posts"
USER_AGENT = "Mozilla/5.0 (compatible; AsyncScraper/1.0; +https://example.com/bot)"
CONCURRENCY = 500               # max concurrent HTTP requests
TCP_LIMIT = CONCURRENCY
MAX_RETRIES = 4
INITIAL_BACKOFF = 0.3
TIMEOUT = aiohttp.ClientTimeout(total=20)
PARSER_THREADPOOL_SIZE = 32
tzinfos = {"EST": -21600}

# -------------------------
# Prepare output directories
# -------------------------
os.makedirs(POSTS_DIR, exist_ok=True)

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        writer.writerow(["post_id", "category", "username", "timestamp", "text", "status"])

write_queue: asyncio.Queue = asyncio.Queue()
parser_executor = ThreadPoolExecutor(max_workers=PARSER_THREADPOOL_SIZE)

# -------------------------
# Parsing
# -------------------------
def _parse_post_html(html, target_post_id):
    soup = BeautifulSoup(html, "html.parser")
    category = None
    try:
        category_link = soup.select_one(".linkst ul li:nth-child(2) a")
        if category_link and category_link.has_attr("href"):
            parts = category_link.get("href").strip("/").split("/")
            if len(parts) >= 3 and parts[1] == "category":
                try:
                    category = int(parts[2])
                except: 
                    pass
    except:
        category = None

    results = []
    posts = soup.find_all("div", class_="blockpost")
    for post in posts:
        username_tag = post.find("a", class_="black username")
        username = username_tag.text.strip() if username_tag else None

        timestamp = None
        post_id_real = target_post_id
        try:
            el = post.find("span", class_="conr")
            if el:
                next_anchor = el.find_next("a")
                if next_anchor and next_anchor.has_attr("href"):
                    href = next_anchor["href"]
                    try:
                        parts = href.strip("/").split("/")
                        if "post" in parts:
                            idx = parts.index("post")
                            post_id_real = int(parts[idx + 1])
                    except:
                        pass
                    txt = next_anchor.get_text(strip=True)
                    if txt:
                        try:
                            if "Today" in txt or "Yesterday" in txt:
                                offset = 1 if "Yesterday" in txt else 0
                                tokens = txt.split()
                                if len(tokens) >= 2:
                                    timestr = tokens[1]
                                    base = datetime.now()
                                    date_guess = (base - timedelta(days=offset)).strftime("%Y-%m-%d")
                                    timestamp = int(dateparser.parse(date_guess + " " + timestr + " EST",
                                                                     tzinfos=tzinfos).timestamp())
                            else:
                                timestamp = int(dateparser.parse(txt + " EST", tzinfos=tzinfos).timestamp())
                        except:
                            timestamp = None
        except:
            timestamp = None

        try:
            text_tag = post.find("div", class_="postmsg")
            text = text_tag.get_text("\n", strip=True) if text_tag else None
        except:
            text = None

        results.append({
            "post_id": post_id_real,
            "category": category,
            "username": username,
            "timestamp": timestamp,
            "text": text,
            "status": 200
        })

    if not results:
        results.append({
            "post_id": target_post_id,
            "category": category,
            "username": None,
            "timestamp": None,
            "text": None,
            "status": 204
        })
    return results

# -------------------------
# Fetch post with retries
# -------------------------
async def fetch_once(session: aiohttp.ClientSession, post_id: int):
    url = f"https://scratch.mit.edu/discuss/post/{post_id}"
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            async with session.get(url) as resp:
                status = resp.status
                text = await resp.text()
                return status, text
        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt > MAX_RETRIES:
                return None, None
            await asyncio.sleep(backoff + random.random() * backoff)
            backoff *= 1.8
    return None, None

# -------------------------
# Process post
# -------------------------
async def process_post(session: aiohttp.ClientSession, sem: asyncio.Semaphore, post_id: int, loop: asyncio.AbstractEventLoop):
    async with sem:
        status, html = await fetch_once(session, post_id)

    if status is None:
        record = {"post_id": post_id, "category": None, "username": None, "timestamp": None, "text": None, "status": "timeout"}
        await write_queue.put(record)
        print_record(record)
        return

    if status in (403, 404):
        record = {"post_id": post_id, "category": None, "username": None, "timestamp": None, "text": None, "status": status}
        await write_queue.put(record)
        print_record(record)
        return

    parse_task = loop.run_in_executor(parser_executor, _parse_post_html, html, post_id)
    parsed_posts = await parse_task

    for rec in parsed_posts:
        await write_queue.put(rec)
        await loop.run_in_executor(parser_executor, write_post_txt, rec)
        print_record(rec)

# -------------------------
# Write post text to file
# -------------------------
def write_post_txt(rec):
    if rec.get("text"):
        path = os.path.join(POSTS_DIR, f"{rec['post_id']}.txt")
        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(rec["text"])
        except Exception as e:
            print(f"[TXT write error {rec['post_id']}] {e}", file=sys.stderr)

# -------------------------
# Writer task: CSV
# -------------------------
async def writer_task(stop_event: asyncio.Event):
    loop = asyncio.get_event_loop()
    while True:
        try:
            rec = await asyncio.wait_for(write_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            if stop_event.is_set() and write_queue.empty():
                break
            continue
        await loop.run_in_executor(None, append_row_to_csv, rec)
        write_queue.task_done()
    while not write_queue.empty():
        rec = await write_queue.get()
        await loop.run_in_executor(None, append_row_to_csv, rec)
        write_queue.task_done()

def append_row_to_csv(rec):
    try:
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
            writer.writerow([
                rec.get("post_id"),
                rec.get("category"),
                rec.get("username"),
                rec.get("timestamp"),
                rec.get("text"),
                rec.get("status")
            ])
    except Exception as e:
        print(f"[CSV write error] {e}", file=sys.stderr)

def print_record(rec):
    text_preview = (rec.get("text") or "")[:100].replace("\n", " ").strip()
    stamp = rec.get("timestamp")
    dt = datetime.utcfromtimestamp(stamp).isoformat() if isinstance(stamp, int) else None
    sys.stdout.write(f"POST {rec.get('post_id'):>7} | status={rec.get('status')} | user={rec.get('username')} | time={dt} | text=\"{text_preview}\"\n")
    sys.stdout.flush()

# -------------------------
# Main
# -------------------------
async def main():
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=TCP_LIMIT, limit_per_host=TCP_LIMIT, ssl=False)
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html"}

    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT, headers=headers) as session:
        stop_event = asyncio.Event()
        writer = asyncio.create_task(writer_task(stop_event))
        tasks = []

        chunk_size = CONCURRENCY * 2
        for start in range(START_POST_ID, MAX_POST_ID + 1, chunk_size):
            end = min(start + chunk_size - 1, MAX_POST_ID)
            for pid in range(start, end + 1):
                t = asyncio.create_task(process_post(session, sem, pid, loop))
                tasks.append(t)
            await asyncio.sleep(0.01)
            if len(tasks) > CONCURRENCY * 10:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = list(pending)

        if tasks:
            await asyncio.gather(*tasks)

        await write_queue.join()
        stop_event.set()
        await writer

if __name__ == "__main__":
    start_ts = time.time()
    try:
        asyncio.run(main())
    finally:
        parser_executor.shutdown(wait=True)
        elapsed = time.time() - start_ts
        print(f"Done. Elapsed: {elapsed:.1f}s")
 
