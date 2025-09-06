from psycopg2.extras import execute_batch
from etl.utils import get_conn

UPSERT_ANIME = """
INSERT INTO anime(anime_key,id_src,src,title_romaji,title_english,title_native,format,episodes,status,
                  start_date,end_date,avg_score,popularity,duration,description,cover_image,banner_image)
VALUES (%(anime_key)s,%(id_src)s,%(src)s,%(title_romaji)s,%(title_english)s,%(title_native)s,%(format)s,%(episodes)s,%(status)s,
        %(start_date)s,%(end_date)s,%(avg_score)s,%(popularity)s,%(duration)s,%(description)s,%(cover_image)s,%(banner_image)s)
ON CONFLICT (anime_key) DO UPDATE SET
  title_romaji=EXCLUDED.title_romaji,
  title_english=EXCLUDED.title_english,
  title_native=EXCLUDED.title_native,
  format=EXCLUDED.format,
  episodes=EXCLUDED.episodes,
  status=EXCLUDED.status,
  start_date=EXCLUDED.start_date,
  end_date=EXCLUDED.end_date,
  avg_score=EXCLUDED.avg_score,
  popularity=EXCLUDED.popularity,
  duration=EXCLUDED.duration,
  description=EXCLUDED.description,
  cover_image=EXCLUDED.cover_image,
  banner_image=EXCLUDED.banner_image;
"""

INSERT_GENRE = "INSERT INTO genre(genre) VALUES (%s) ON CONFLICT DO NOTHING;"
INSERT_ANIME_GENRE = "INSERT INTO anime_genre(anime_key, genre) VALUES (%s,%s) ON CONFLICT DO NOTHING;"

def upsert_anime(rows):
    with get_conn() as conn, conn.cursor() as cur:
        execute_batch(cur, UPSERT_ANIME, rows, page_size=500)
        for r in rows:
            for g in r["genres"]:
                cur.execute(INSERT_GENRE, (g,))
                cur.execute(INSERT_ANIME_GENRE, (r["anime_key"], g))
        conn.commit()
