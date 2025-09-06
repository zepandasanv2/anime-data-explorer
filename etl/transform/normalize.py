from datetime import date

def _ymd(d):
    if not d or not d.get("year"):
        return None
    y, m, dd = d["year"], d.get("month") or 1, d.get("day") or 1
    return date(y, m, dd)

def normalize_media(m):
    return {
        "anime_key": f"anilist:{m['id']}",
        "id_src": str(m["id"]),
        "src": "anilist",
        "title_romaji": (m.get("title") or {}).get("romaji"),
        "title_english": (m.get("title") or {}).get("english"),
        "title_native": (m.get("title") or {}).get("native"),
        "format": m.get("format"),
        "episodes": m.get("episodes"),
        "status": m.get("status"),
        "start_date": _ymd(m.get("startDate")),
        "end_date": _ymd(m.get("endDate")),
        "avg_score": m.get("averageScore"),
        "popularity": m.get("popularity"),
        "duration": m.get("duration"),
        "description": m.get("description"),
        "cover_image": (m.get("coverImage") or {}).get("large"),
        "banner_image": m.get("bannerImage"),
        "genres": m.get("genres") or []
    }
