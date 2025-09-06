import time
import requests

ENDPOINT = "https://graphql.anilist.co"

QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage currentPage }
    media(type: ANIME, sort: POPULARITY_DESC) {
      id
      title { romaji english native }
      format
      episodes
      status
      startDate { year month day }
      endDate { year month day }
      averageScore
      popularity
      duration
      description(asHtml: false)
      coverImage { large }
      bannerImage
      genres
    }
  }
}
"""

class AniListClient:
    def __init__(self, endpoint=ENDPOINT, max_retries=3, backoff=1.5, timeout=30):
        self.endpoint = endpoint
        self.max_retries = max_retries
        self.backoff = backoff
        self.timeout = timeout

    def fetch_page(self, page=1, per_page=50):
        payload = {"query": QUERY, "variables": {"page": page, "perPage": per_page}}
        attempt = 0
        while True:
            try:
                r = requests.post(self.endpoint, json=payload, timeout=self.timeout)
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", "2"))
                    time.sleep(retry_after)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                time.sleep(self.backoff ** attempt)
