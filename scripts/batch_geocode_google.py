#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch geocode dm-Markt addresses with Google Maps Geocoding API.

Usage:
  pip install googlemaps pandas tenacity python-dotenv openpyxl
  (w Actions używamy sekretu GOOGLE_MAPS_API_KEY)
"""

import os, sys, argparse, time
from pathlib import Path
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    import googlemaps
except ImportError:
    print("Missing dependency. Please run: pip install googlemaps")
    sys.exit(1)

def parse_sheet(xlsx_path: Path, sheet_name: str, country_label: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
    rows = []
    for _, row in df.iterrows():
        first = row[0]
        if pd.isna(first) or str(first).strip() in ["dm-Markt", "Gesamt"]:
            continue
        street = row[3]
        if pd.isna(street) or str(street).strip() == "Strasse":
            continue
        plz = str(row[4]).split(".")[0] if not pd.isna(row[4]) else ""
        city = str(row[5]) if not pd.isna(row[5]) else ""
        code = str(first).strip()
        street_str = str(street).strip()
        addr = f"{street_str}, {plz} {city}, {country_label}"
        rows.append({
            "dm_code": code,
            "Strasse": street_str,
            "PLZ": plz,
            "Ort": city,
            "Country": country_label,
            "address_for_geocoding": addr,
        })
    return pd.DataFrame(rows)

def load_or_create_cache(cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        try:
            return pd.read_csv(cache_path)
        except Exception:
            pass
    return pd.DataFrame(columns=["address_for_geocoding","latitude","longitude","place_id","geocode_status"])

def save_cache(cache_df: pd.DataFrame, cache_path: Path):
    cache_df.drop_duplicates(subset=["address_for_geocoding"], keep="last").to_csv(cache_path, index=False)

class RateLimitError(Exception): ...
@retry(
    retry=retry_if_exception_type((RateLimitError, )),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(6),
    reraise=True
)
def geocode_one(gmaps_client, address: str):
    try:
        results = gmaps_client.geocode(address, language="de")
    except Exception as e:
        if "OVER_QUERY_LIMIT" in str(e) or "OVER_DAILY_LIMIT" in str(e):
            raise RateLimitError(str(e))
        raise
    if not results:
        return None
    best = results[0]
    loc = best["geometry"]["location"]
    return {
        "latitude": loc.get("lat"),
        "longitude": loc.get("lng"),
        "place_id": best.get("place_id"),
        "formatted_address": best.get("formatted_address")
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Excel with sheets 'dm DE' and 'dm AT'")
    ap.add_argument("--output", required=True, help="Output Excel with coords")
    ap.add_argument("--cache", default="geocode_cache.csv", help="CSV cache path")
    ap.add_argument("--sleep", type=float, default=0.05, help="Sleep between calls (s)")
    args = ap.parse_args()

    xlsx_path = Path(args.input)
    out_path = Path(args.output)
    cache_path = Path(args.cache)

    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_MAPS_API_KEY not set.")
        sys.exit(2)

    gmaps_client = googlemaps.Client(key=api_key)

    de = parse_sheet(xlsx_path, "dm DE", "Germany")
    at = parse_sheet(xlsx_path, "dm AT", "Austria")
    combined = pd.concat([de, at], ignore_index=True)

    cache_df = load_or_create_cache(cache_path)
    merged = combined.merge(cache_df, on="address_for_geocoding", how="left", suffixes=("", "_cache"))
    to_fetch = merged[merged["latitude"].isna()].copy()

    results = []
    for _, row in to_fetch.iterrows():
        addr = row["address_for_geocoding"]
        try:
            data = geocode_one(gmaps_client, addr)
            if data is None:
                results.append({
                    "address_for_geocoding": addr,
                    "latitude": None,
                    "longitude": None,
                    "place_id": None,
                    "geocode_status": "NOT_FOUND"
                })
            else:
                results.append({
                    "address_for_geocoding": addr,
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                    "place_id": data["place_id"],
                    "geocode_status": "OK"
                })
        except RateLimitError:
            if results:
                new_df = pd.DataFrame(results)
                cache_df = pd.concat([cache_df, new_df], ignore_index=True)
                save_cache(cache_df, cache_path)
            print("Rate limit hit. Progress saved to cache. Re-run later.")
            sys.exit(3)
        except Exception as e:
            results.append({
                "address_for_geocoding": addr,
                "latitude": None,
                "longitude": None,
                "place_id": None,
                "geocode_status": f"ERROR: {e}"
            })
        time.sleep(args.sleep)

    if results:
        new_df = pd.DataFrame(results)
        cache_df = pd.concat([cache_df, new_df], ignore_index=True)
        save_cache(cache_df, cache_path)

    final = combined.merge(cache_df, on="address_for_geocoding", how="left")
    final.rename(columns={"longitude": "długość", "latitude": "szerokość"}, inplace=True)

    final_de = final[final["Country"] == "Germany"].copy()
    final_at = final[final["Country"] == "Austria"].copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        final_de.to_excel(writer, index=False, sheet_name="dm DE (with coords)")
        final_at.to_excel(writer, index=False, sheet_name="dm AT (with coords)")

    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
