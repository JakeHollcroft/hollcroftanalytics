import os
import json
import requests

API_KEY = os.environ.get("HOUSECALLPRO_API_KEY", "").strip()
HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "HollcroftAnalytics/1.0 (+https://hollcroftanalytics.com)",
}

BASE_URL_PRICEBOOK_SERVICES = "https://api.housecallpro.com/pricebook/services"
BASE_URL_PRICEBOOK_MATERIALS = "https://api.housecallpro.com/pricebook/materials"

def hit(url):
    resp = requests.get(url, headers=HEADERS, params={"page": 1, "page_size": 10})
    print("\nURL:", url)
    print("Status:", resp.status_code)
    print("First 300 chars:", resp.text[:300])

    try:
        data = resp.json()
    except Exception as e:
        print("JSON parse failed:", e)
        return

    print("Top-level keys:", list(data.keys())[:50])

    # common shapes
    for k in ["services", "materials", "data", "items", "results"]:
        v = data.get(k)
        if isinstance(v, list):
            print(f"Found list under key '{k}' with len={len(v)}")
        elif isinstance(v, dict):
            print(f"Found dict under key '{k}' with keys={list(v.keys())[:25]}")

hit(BASE_URL_PRICEBOOK_SERVICES)
hit(BASE_URL_PRICEBOOK_MATERIALS)