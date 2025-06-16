import requests

stats = []
i = 0
while True:
    res = requests.get(f'https://api.scratch.mit.edu/projects/{i}')
    if res.status_code != 200:
        print(f"{i}: 404 Not Found")
    else:
        stats.append(res.json())
        print(stats[-1])  # Safely print the last item added
    i += 1
