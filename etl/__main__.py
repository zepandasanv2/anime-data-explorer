from etl.extract.anilist import AniListClient
from etl.transform.normalize import normalize_media
from etl.load.load_pg import upsert_anime

def run(limit_pages=1, per_page=20):
    client = AniListClient()
    page = 1
    total = 0
    while page <= limit_pages:
        data = client.fetch_page(page=page, per_page=per_page)
        page_info = data["data"]["Page"]["pageInfo"]
        media = data["data"]["Page"]["media"]
        rows = [normalize_media(m) for m in media]
        upsert_anime(rows)
        total += len(rows)
        print(f"Page {page} -> {len(rows)} animes chargés (cumul {total})")
        if not page_info["hasNextPage"]:
            break
        page += 1

if __name__ == "__main__":
    run()
