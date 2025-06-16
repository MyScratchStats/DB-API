import requests

stats = []
i = 0
while True:
  res = requests.get(f'https://api.scratch.mit.edu/projects/{i}')
  if res.status_code != 200:
    print("404 Not Found")
  else:
    stats[i] = res.json()
    print(stats)
  i += 1

