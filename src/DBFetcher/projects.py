import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

stats = []
i = 0
batch_size = 30  # Number of concurrent requests per batch

def fetch_project(pid):
    res = requests.get(f'https://api.scratch.mit.edu/projects/{pid}')
    if res.status_code == 200:
        return res.json()
    return None

while True:
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = {executor.submit(fetch_project, pid): pid for pid in range(i, i + batch_size)}
        for future in as_completed(futures):
            result = future.result()
            if result:
                stats.append(result)
                print(result)

    i += batch_size  # Move to the next batch
