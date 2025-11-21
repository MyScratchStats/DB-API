import requests
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import csv
import os
import time

# -----------------------------
# Configuration
# -----------------------------
CSV_FILE = "scratch_posts.csv"
THREAD_COUNT = 300
BATCH_SIZE = 1000         # each thread grabs 1000 posts per batch
MAX_POST_ID = 10000
tzinfos = {"EST": -21600}

post_data = []
lock = Lock()
batch_lock = Lock()
next_batch_start = 1      # shared counter for the next batch start

# -----------------------------
# Helper functions
# -----------------------------
def fetch_post(post_id):
    url = f"https://scratch.mit.edu/discuss/post/{post_id}"
    for _ in range(5):
        try:
            resp = requests.get(url, timeout=15)
            return resp.status_code, resp.text
        except:
            time.sleep(2)
    return None, None

def process_post(post_id):
    global post_data
    status, html = fetch_post(post_id)
    if status is None:
        with lock:
            post_data.append({"post_id": post_id, "category": None, "username": None,
                              "timestamp": None, "text": None, "status": "timeout"})
        return
    if status in (404, 403):
        with lock:
            post_data.append({"post_id": post_id, "category": None, "username": None,
                              "timestamp": None, "text": None, "status": status})
        return

    root = BeautifulSoup(html, "html.parser")
    target_post = root.select_one(f"#p{post_id}")
    if not target_post:
        return

    category_link = root.select_one(".linkst ul li:nth-child(2) a")
    category = int(category_link.get("href").split("/")[2]) if category_link else None

    posts = root.find_all("div", class_="blockpost")
    for post in posts:
        username_tag = post.find("a", class_="black username")
        username = username_tag.text if username_tag else None

        el = post.find("span", class_="conr").next_sibling
        post_id_real = int(el.get("href").split("/")[3]) if el else post_id

        timestamp = None
        if el:
            try:
                txt = el.text
                if "Today" in txt or "Yesterday" in txt:
                    offset = 1 if "Yesterday" in txt else 0
                    date_str = txt.split(" ")[1]
                    timestamp = int((parser.parse(date_str + " EST", tzinfos=tzinfos)
                                     - timedelta(days=offset)).timestamp())
                else:
                    timestamp = int(parser.parse(txt + " EST", tzinfos=tzinfos).timestamp())
            except:
                timestamp = None

        text_tag = post.find("div", class_="postmsg")
        text = text_tag.get_text("\n", strip=True) if text_tag else None

        with lock:
            post_data.append({
                "post_id": post_id_real,
                "category": category,
                "username": username,
                "timestamp": timestamp,
                "text": text,
                "status": 200
            })

# -----------------------------
# Batch worker
# -----------------------------
def worker():
    global next_batch_start
    while True:
        with batch_lock:
            if next_batch_start > MAX_POST_ID:
                return  # all batches done
            start_id = next_batch_start
            end_id = min(start_id + BATCH_SIZE - 1, MAX_POST_ID)
            next_batch_start = end_id + 1

        # process this batch
        print(f"Thread {threading.get_ident()} processing {start_id} → {end_id}")
        for pid in range(start_id, end_id + 1):
            process_post(pid)

        # save progress after each batch
        save_csv()

# -----------------------------
# Save CSV
# -----------------------------
def save_csv():
    output_path = os.path.join(os.getcwd(), CSV_FILE)
    with lock:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["post_id", "category", "username", "timestamp", "text", "status"])
            for post in post_data:
                writer.writerow([
                    post["post_id"],
                    post["category"],
                    post["username"],
                    post["timestamp"],
                    post["text"],
                    post["status"]
                ])
    print(f"CSV saved ({len(post_data)} posts)")

# -----------------------------
# Main
# -----------------------------
import threading

def main():
    print(f"Starting scraper up to {MAX_POST_ID} using {THREAD_COUNT} threads")
    threads = []
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("Scraping complete.")

if __name__ == "__main__":
    main()
