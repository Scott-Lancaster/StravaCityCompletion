#!/usr/bin/env python3

"""
CityCompletion.py

Features
--------
- Downloads Strava activities into SQLite
- Builds city street inventory from OpenStreetMap
- Creates raw GPS overlay map
- Creates completion map
- Progress output during heavy processing
- --buffer-ft for GPS drift tolerance
- --street-buffer-ft for sidewalk / parallel route forgiveness
- Prints completion by street count
- Prints completion by total mileage

Usage
-----
python3 CityCompletion.py "San Francisco"
python3 CityCompletion.py "San Francisco" --update
python3 CityCompletion.py "San Francisco" --buffer-ft 60
python3 CityCompletion.py "San Francisco" --street-buffer-ft 35
"""

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

import folium
import geopandas as gpd
import osmnx as ox
import pandas as pd
import polyline
import requests
from shapely.geometry import LineString
from shapely.ops import unary_union


# ==========================================================
# CONFIG
# ==========================================================

CONFIG_FILE = "config.json"


# ==========================================================
# HELPERS
# ==========================================================

def sanitize_name(name):
    clean = re.sub(r"[^a-zA-Z0-9\s]", "", name)
    clean = clean.strip().replace(" ", "_").lower()
    return clean


def today_tag():
    return datetime.now().strftime("%Y%m%d")


def feet_to_meters(feet):
    return feet * 0.3048


def meters_to_miles(meters):
    return meters / 1609.344


def load_config(path):
    if not os.path.exists(path):
        blank = {
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "REFRESH_TOKEN": ""
        }

        with open(path, "w", encoding="utf-8") as file:
            json.dump(blank, file, indent=4)

        print("Created blank config.json")
        print("Please populate your Strava credentials.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as file:
        config = json.load(file)

    required = [
        "CLIENT_ID",
        "CLIENT_SECRET",
        "REFRESH_TOKEN"
    ]

    for key in required:
        if not config.get(key):
            print(f"Missing config value: {key}")
            sys.exit(1)

    return config


def latest_strava_db():
    matches = []

    for file_name in os.listdir("."):
        if re.match(r"strava_activities_\d{8}\.db$", file_name):
            matches.append(file_name)

    if not matches:
        return None

    matches.sort(reverse=True)
    return matches[0]


# ==========================================================
# STRAVA
# ==========================================================

def refresh_access_token(config):
    url = "https://www.strava.com/api/v3/oauth/token"

    payload = {
        "client_id": config["CLIENT_ID"],
        "client_secret": config["CLIENT_SECRET"],
        "grant_type": "refresh_token",
        "refresh_token": config["REFRESH_TOKEN"]
    }

    response = requests.post(
        url,
        data=payload,
        timeout=20
    )

    response.raise_for_status()

    token = response.json()["access_token"]

    return token


def fetch_activity_pages(headers):
    rows = []
    page = 1

    while True:
        params = {
            "page": page,
            "per_page": 200
        }

        response = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=headers,
            params=params,
            timeout=20
        )

        if response.status_code == 429:
            print("Rate limited. Sleeping 60 sec...")
            time.sleep(60)
            continue

        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        rows.extend(batch)

        print(f"Fetched page {page}: {len(batch)} rows")

        page += 1
        time.sleep(0.7)

    return rows


def fetch_detail_polyline(activity_id, headers):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=20
        )

        if response.status_code == 429:
            return None

        response.raise_for_status()

        data = response.json()

        route = data.get("map", {})

        poly = route.get("polyline")

        if poly:
            return poly

        return route.get("summary_polyline")

    except Exception:
        return None


def create_activity_table(connection):
    sql = """
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY,
        date TEXT,
        name TEXT,
        sport_type TEXT,
        distance_m REAL,
        moving_time_sec INTEGER,
        polyline TEXT,
        timestamp TEXT
    )
    """

    connection.execute(sql)
    connection.commit()


def insert_activities(connection, rows, headers):
    cursor = connection.cursor()

    inserted = 0

    allowed = {
        "run",
        "walk",
        "hike",
        "ride",
        "bike",
        "virtualride"
    }

    for activity in rows:
        sport = (
            activity.get("sport_type")
            or activity.get("type", "")
        ).lower()

        if sport not in allowed:
            continue

        date_value = activity.get(
            "start_date_local",
            ""
        )[:10]

        if not date_value:
            continue

        poly = activity.get(
            "map",
            {}
        ).get("summary_polyline")

        if not poly:
            poly = fetch_detail_polyline(
                activity["id"],
                headers
            )

        if not poly:
            continue

        values = (
            activity["id"],
            date_value,
            activity.get("name"),
            sport,
            activity.get("distance"),
            activity.get("moving_time"),
            poly,
            datetime.now().isoformat()
        )

        cursor.execute(
            """
            INSERT OR IGNORE INTO activities (
                id,
                date,
                name,
                sport_type,
                distance_m,
                moving_time_sec,
                polyline,
                timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values
        )

        inserted += 1

        if inserted % 50 == 0:
            connection.commit()

    connection.commit()

    return inserted


def build_strava_database(config, update_flag):
    if not update_flag:
        existing = latest_strava_db()

        if existing:
            print(f"Using existing Strava DB: {existing}")
            return existing

    db_name = f"strava_activities_{today_tag()}.db"

    print("Building Strava DB...")

    token = refresh_access_token(config)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    rows = fetch_activity_pages(headers)

    connection = sqlite3.connect(db_name)

    create_activity_table(connection)

    inserted = insert_activities(
        connection,
        rows,
        headers
    )

    connection.close()

    print(f"Inserted {inserted} valid activities")

    return db_name


# ==========================================================
# CITY STREETS
# ==========================================================

def build_city_database(city_name):
    safe = sanitize_name(city_name)
    db_name = f"{safe}_completion.db"

    if os.path.exists(db_name):
        return db_name

    print("Downloading city streets from OSM...")

    query = f"{city_name}, California, USA"

    graph = ox.graph_from_place(
        query,
        network_type="drive",
        simplify=True
    )

    edges = ox.graph_to_gdfs(
        graph,
        nodes=False,
        edges=True
    )

    connection = sqlite3.connect(db_name)

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS streets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            street_name TEXT,
            geom_json TEXT
        )
        """
    )

    cursor = connection.cursor()

    inserted = 0

    for _, row in edges.iterrows():
        try:
            coords = list(row.geometry.coords)
            geom_json = json.dumps(coords)

            street_name = row.get("name")

            if isinstance(street_name, list):
                street_name = ", ".join(street_name)

            if not street_name:
                street_name = "Unnamed"

            cursor.execute(
                """
                INSERT INTO streets (
                    street_name,
                    geom_json
                )
                VALUES (?, ?)
                """,
                (
                    street_name,
                    geom_json
                )
            )

            inserted += 1

        except Exception:
            continue

    connection.commit()
    connection.close()

    print(f"Stored {inserted} street segments")

    return db_name


# ==========================================================
# LOADERS
# ==========================================================

def load_streets(db_name):
    connection = sqlite3.connect(db_name)

    rows = connection.execute(
        """
        SELECT street_name, geom_json
        FROM streets
        """
    ).fetchall()

    connection.close()

    return rows


def load_routes(db_name):
    connection = sqlite3.connect(db_name)

    rows = connection.execute(
        """
        SELECT polyline, name
        FROM activities
        WHERE polyline IS NOT NULL
        AND TRIM(polyline) != ''
        AND LENGTH(polyline) > 10
        """
    ).fetchall()

    connection.close()

    return rows


# ==========================================================
# MAPS
# ==========================================================

def create_raw_map(city_name, streets, routes):
    safe = sanitize_name(city_name)

    m = folium.Map(
        location=[37.7749, -122.4194],
        zoom_start=13,
        tiles="CartoDB positron"
    )

    for street_name, geom_json in streets:
        try:
            coords = json.loads(geom_json)

            points = [
                (lat, lon)
                for lon, lat in coords
            ]

            folium.PolyLine(
                points,
                color="#999999",
                weight=1.3,
                opacity=0.65
            ).add_to(m)

        except Exception:
            continue

    drawn = 0
    failed = 0

    for poly, route_name in routes:
        try:
            points = polyline.decode(poly)

            if len(points) < 2:
                failed += 1
                continue

            folium.PolyLine(
                points,
                color="#FC4C02",
                weight=3,
                opacity=0.85,
                popup=route_name
            ).add_to(m)

            drawn += 1

        except Exception:
            failed += 1

    file_name = f"{safe}_raw_gps.html"

    m.save(file_name)

    print(f"Routes drawn: {drawn}")
    print(f"Routes failed: {failed}")
    print(f"Saved {file_name}")


# ==========================================================
# COMPLETION
# ==========================================================

def create_completion_map(
    city_name,
    streets,
    routes,
    gps_buffer_ft,
    street_buffer_ft
):
    safe = sanitize_name(city_name)

    print("Preparing streets...")

    street_rows = []

    for street_name, geom_json in streets:
        try:
            coords = json.loads(geom_json)

            line = LineString(coords)

            street_rows.append(
                {
                    "name": street_name,
                    "geometry": line
                }
            )

        except Exception:
            continue

    print("Preparing routes...")

    route_lines = []

    for poly, _ in routes:
        try:
            pts = polyline.decode(poly)

            if len(pts) >= 2:
                line = LineString(
                    [
                        (lon, lat)
                        for lat, lon in pts
                    ]
                )

                route_lines.append(line)

        except Exception:
            continue

    street_gdf = gpd.GeoDataFrame(
        street_rows,
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    route_gdf = gpd.GeoDataFrame(
        geometry=route_lines,
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    print("Building route spatial index...")

    route_index = route_gdf.sindex

    gps_buffer_m = feet_to_meters(gps_buffer_ft)
    street_buffer_m = feet_to_meters(street_buffer_ft)

    street_gdf["covered"] = False
    street_gdf["coverage_ratio"] = 0.0
    street_gdf["length_m"] = street_gdf.geometry.length

    total = len(street_gdf)

    print("Calculating completion...")

    for i, row in street_gdf.iterrows():
        street = row.geometry

        expanded = street.buffer(
            gps_buffer_m + street_buffer_m
        )

        nearby_ids = list(
            route_index.intersection(
                expanded.bounds
            )
        )

        if not nearby_ids:
            if i % 500 == 0:
                pct = round(i / total * 100, 1)
                print(
                    f"Progress {pct}% ({i}/{total})"
                )
            continue

        nearby_routes = route_gdf.iloc[nearby_ids]

        covered_length = 0.0
        total_length = street.length

        for _, route_row in nearby_routes.iterrows():
            route_buffer = route_row.geometry.buffer(
                gps_buffer_m + street_buffer_m
            )

            overlap = street.intersection(
                route_buffer
            )

            covered_length += overlap.length

        ratio = min(
            covered_length / total_length,
            1.0
        )

        street_gdf.at[
            i,
            "coverage_ratio"
        ] = ratio

        if ratio >= 0.75:
            street_gdf.at[
                i,
                "covered"
            ] = True

        if i % 500 == 0 or i == total - 1:
            pct = round(i / total * 100, 1)

            covered_now = int(
                street_gdf["covered"].sum()
            )

            print(
                f"Progress {pct}% "
                f"({i}/{total}) "
                f"Covered Segments: {covered_now}"
            )

    covered_segments = int(
        street_gdf["covered"].sum()
    )

    segment_pct = round(
        covered_segments / total * 100,
        1
    )

    total_meters = street_gdf["length_m"].sum()

    covered_meters = street_gdf.loc[
        street_gdf["covered"],
        "length_m"
    ].sum()

    total_miles = meters_to_miles(
        total_meters
    )

    covered_miles = meters_to_miles(
        covered_meters
    )

    mileage_pct = round(
        covered_meters / total_meters * 100,
        1
    )

    print("")
    print("============== RESULTS ==============")
    print(
        f"Street Segment Completion: "
        f"{segment_pct}% "
        f"({covered_segments}/{total})"
    )
    print(
        f"Mileage Completion: "
        f"{mileage_pct}% "
        f"({covered_miles:.1f} / {total_miles:.1f} mi)"
    )
    print("====================================")
    print("")

    street_gdf = street_gdf.to_crs(
        epsg=4326
    )

    print("Rendering completion map...")

    m = folium.Map(
        location=[37.7749, -122.4194],
        zoom_start=13,
        tiles="CartoDB positron"
    )

    for _, row in street_gdf.iterrows():
        coords = [
            (lat, lon)
            for lon, lat in row.geometry.coords
        ]

        color = (
            "#FC4C02"
            if row["covered"]
            else "#888888"
        )

        weight = (
            4
            if row["covered"]
            else 1.5
        )

        popup = (
            f"{row['name']}<br>"
            f"Coverage: "
            f"{row['coverage_ratio'] * 100:.1f}%"
        )

        folium.PolyLine(
            coords,
            color=color,
            weight=weight,
            opacity=0.9,
            popup=popup
        ).add_to(m)

    file_name = f"{safe}_completion.html"

    m.save(file_name)

    print(f"Saved {file_name}")


# ==========================================================
# CLI
# ==========================================================

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "city",
        type=str,
        help="City name"
    )

    parser.add_argument(
        "--update",
        action="store_true"
    )

    parser.add_argument(
        "--buffer-ft",
        type=float,
        default=50,
        help="GPS drift buffer"
    )

    parser.add_argument(
        "--street-buffer-ft",
        type=float,
        default=25,
        help="Street snap tolerance"
    )

    return parser.parse_args()


# ==========================================================
# MAIN
# ==========================================================

def main():
    args = parse_args()

    config = load_config(CONFIG_FILE)

    city_db = build_city_database(
        args.city
    )

    strava_db = build_strava_database(
        config,
        args.update
    )

    streets = load_streets(city_db)

    routes = load_routes(strava_db)

    create_raw_map(
        args.city,
        streets,
        routes
    )

    create_completion_map(
        args.city,
        streets,
        routes,
        args.buffer_ft,
        args.street_buffer_ft
    )


if __name__ == "__main__":
    main()