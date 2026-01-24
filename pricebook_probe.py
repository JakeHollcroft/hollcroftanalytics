import os
import json
import requests

API_KEY = os.environ.get("HOUSECALLPRO_API_KEY", "").strip()
URL = "https://api.housecallpro.com/api/price_book/services"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Token {API_KEY}",
}

params = {
    "page": 1,
    "page_size": 10,
    "sort_by": "created_at",
    "sort_direction": "desc",
}

r = requests.get(URL, headers=headers, params=params, timeout=30)
print("status:", r.status_code)
print("content-type:", r.headers.get("content-type"))

try:
    data = r.json()
except Exception:
    print("non-json body preview:", r.text[:500])
    raise

print("top-level keys:", list(data.keys())[:50])

# doc shape: {"checklists": [{page, page_size, total_pages, total_items, data:[...] }]}
checklists = data.get("checklists") or []
print("checklists len:", len(checklists))

if checklists:
    first = checklists[0]
    print("page:", first.get("page"))
    print("page_size:", first.get("page_size"))
    print("total_pages:", first.get("total_pages"))
    print("total_items:", first.get("total_items"))
    rows = first.get("data") or []
    print("rows in first checklist:", len(rows))
    if rows:
        print("first row keys:", list(rows[0].keys())[:50])
        print("first row preview:", json.dumps(rows[0], indent=2)[:800])
else:
    print("payload preview:", json.dumps(data, indent=2)[:800])
