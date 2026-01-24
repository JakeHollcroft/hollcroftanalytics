import os
import json
import requests

API_KEY = os.environ.get("HOUSECALLPRO_API_KEY", "").strip()
HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "HollcroftAnalytics/1.0 (+https://hollcroftanalytics.com)",
}

URLS = [
    "https://api.housecallpro.com/api/price_book/services",
    "https://api.housecallpro.com/api/price_book/materials",
    # keep your previous ones too, just to confirm:
    "https://api.housecallpro.com/pricebook/services",
    "https://api.housecallpro.com/pricebook/materials",
]

for url in URLS:
    print("\nURL:", url)
    r = requests.get(url, headers=HEADERS, timeout=30)
    print("Status:", r.status_code)
    print("Content-Type:", r.headers.get("content-type"))

    # Show a little body either way
    print("First 250 chars:", r.text[:250].replace("\n", " "))

    if r.status_code == 200:
        try:
            j = r.json()
            print("Top-level keys:", list(j)[:20] if isinstance(j, dict) else type(j))
        except Exception as e:
            print("JSON parse failed:", e)