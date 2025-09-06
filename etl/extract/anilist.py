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

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    # un User-Agent explicite évite 403/anti-bot
    "User-Agent": "AnimeDataExplorer/1.0 (+https://github.com/TON_USER/anime-data-explorer)",
    # Origin/Referer aident selon l’environnement réseau
    "Origin": "https://anilist.co",
    "Referer": "https://anilist.co",
}

class AniListClient:
    def __init__(self, endpoint=ENDPOINT, max_retries=3, backoff=1.5, timeout=30, headers=None):
        self.endpoint = endpoint
        self.max_retries = max_retries
        self.backoff = backoff
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS if headers is None else headers)

    def fetch_page(self, page=1, per_page=50):
        payload = {"query": QUERY, "variables": {"page": page, "perPage": per_page}}
        attempt = 0
        while True:
            try:
                r = self.session.post(self.endpoint, json=payload, timeout=self.timeout)
                if r.status_code in (429, 503):
                    # rate limit / protection temporaire
                    retry_after = int(r.headers.get("Retry-After", "2"))
                    time.sleep(retry_after or self.backoff ** (attempt + 1))
                    attempt += 1
                    continue
                if r.status_code == 403:
                    # souvent headers/anti-bot : on retente une fois avec un UA différent
                    self.session.headers.update({"User-Agent": f"AnimeDataExplorer-Retry/{int(time.time())}"})
                    attempt += 1
                    if attempt <= self.max_retries:
                        time.sleep(self.backoff ** attempt)
                        continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                attempt += 1
                if attempt > self.max_retries:
                    # Aidant au debug
                    msg = getattr(e.response, "text", None)
                    raise RuntimeError(f"AniList request failed ({e}). Body={msg!r}") from e
                time.sleep(self.backoff ** attempt)
