import os, sys
import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS anime(
  anime_key TEXT PRIMARY KEY,
  id_src TEXT NOT NULL,
  src TEXT NOT NULL,
  title_romaji TEXT, title_english TEXT, title_native TEXT,
  format TEXT, episodes INT, status TEXT,
  start_date DATE, end_date DATE,
  avg_score INT, popularity INT, duration INT,
  description TEXT, cover_image TEXT, banner_image TEXT
);
CREATE TABLE IF NOT EXISTS genre(genre TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS anime_genre(
  anime_key TEXT REFERENCES anime(anime_key),
  genre TEXT REFERENCES genre(genre),
  PRIMARY KEY (anime_key, genre)
);
"""

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "anime_db"),
        user=os.getenv("PGUSER", "anime"),
        password=os.getenv("PGPASSWORD", "anime"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", 5432)),
        connect_timeout=5
    )

if __name__ == "__main__":
    if "--init-db" in sys.argv:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
        print("DB initialisée")
