#!/usr/bin/env python3
"""
CityCompletion.py

DESCRIPTION:
    Robust Strava city street coverage analyzer.

    FEATURES:
    - Grey map created ONLY if missing
    - Only ONE dated strava_activities_YYYYMMDD.db + .csv
    - Incremental fetch on --update with 429 rate-limit retry
    - Exact fractional geometry overlap (solves the intersection tripwire bug)
    - Auto-detects and upgrades old database schemas
    - Consolidates divided highways into single lines
    - Configurable GPS buffer width via --buffer-ft
    - Prints Top 10 longest uncompleted streets
    - Interactive CLI Debugger via --debug flag

USAGE:
    python3 CityCompletion.py "San Francisco"
    python3 CityCompletion.py "San Francisco" --update
    python3 CityCompletion.py "San Francisco" --buffer-ft 75
    python3 CityCompletion.py "San Francisco" --debug
"""

import requests
import json
import time
import sqlite3
import sys
import os
import folium
import polyline
import re
import osmnx as ox
import pandas as pd
from shapely.geometry import LineString
from shapely.ops import unary_union
import geopandas as gpd
from datetime import datetime

# ========================= CONFIGURATION =========================
CONFIG_FILE = "config.json"

if not os.path.exists(CONFIG_FILE):
    print(f"⚠️  {CONFIG_FILE} not found. Creating blank config...")
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"CLIENT_ID": "", "CLIENT_SECRET": "", "REFRESH_TOKEN": "", "YEAR": 2026}, f, indent=4)
    print("Please fill in your credentials.")
    sys.exit(1)

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

CLIENT_ID = config.get("CLIENT_ID", "")
CLIENT_SECRET = config.get("CLIENT_SECRET", "")
REFRESH_TOKEN = config.get("REFRESH_TOKEN", "")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
    print("❌ Missing credentials in config.json")
    sys.exit(1)

# Parse Arguments
buffer_ft = 50.0 # Default 50 feet
if "--buffer-ft" in sys.argv:
    try:
        idx = sys.argv.index("--buffer-ft")
        buffer_ft = float(sys.argv[idx + 1])
    except:
        pass
buffer_m = buffer_ft * 0.3048

# ================================================================

def sanitize_name(name):
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    name = name.strip().replace(' ', '_').lower()
    return name

def get_today_tag():
    return datetime.now().strftime("%Y%m%d")

def refresh_access_token():
    print("🔑 Refreshing Strava access token...")
    response = requests.post("https://www.strava.com/api/v3/oauth/token", data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    })
    response.raise_for_status()
    print("✅ Token refreshed")
    return response.json()["access_token"]

def create_strava_activities_db():
    today_tag = get_today_tag()
    db_file = f"strava_activities_{today_tag}.db"
    csv_file = f"strava_activities_{today_tag}.csv"
    
    print(f"🔄 Step 2/5: Creating new Strava activities database: {db_file}")

    for f in os.listdir('.'):
        if f.startswith("strava_activities_") and f.endswith((".db", ".csv")) and today_tag not in f:
            try:
                os.remove(f)
                print(f"   🗑️  Deleted old file: {f}")
            except:
                pass

    conn = sqlite3.connect(db_file)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY,
            date TEXT,
            name TEXT,
            sport_type TEXT,
            distance_m REAL,
            moving_time_sec INTEGER,
            start_lat REAL,
            start_lon REAL,
            end_lat REAL,
            end_lon REAL,
            location_city TEXT,
            location_state TEXT,
            timezone TEXT,
            polyline TEXT,
            raw_json TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()

    last_date = conn.execute("SELECT MAX(date) FROM activities").fetchone()[0]
    after_ts = int(datetime.fromisoformat(last_date).timestamp()) if last_date else 0
    print(f"   Fetching activities after: {last_date or 'the beginning'}")

    access_token = refresh_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    activities = []
    page = 1
    while True:
        params = {"after": after_ts, "per_page": 200, "page": page}
        try:
            r = requests.get("https://www.strava.com/api/v3/athlete/activities", headers=headers, params=params, timeout=15)
            if r.status_code == 429:
                print("⏳ Rate limit hit (429) — waiting 60 seconds...")
                time.sleep(60)
                continue
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            activities.extend(batch)
            print(f"   Page {page}: +{len(batch)} activities")
            time.sleep(0.7)
            page += 1
        except Exception as e:
            print(f"   Error on page {page}: {e}")
            time.sleep(10)
            continue

    saved = 0
    for act in activities:
        sport = (act.get("sport_type") or act.get("type", "")).lower()
        if sport not in ["run", "walk", "hike", "ride", "bike", "virtualride"]:
            continue
        date_str = act.get("start_date_local", "")[:10]
        if not date_str:
            continue

        try:
            detail_url = f"https://www.strava.com/api/v3/activities/{act['id']}"
            r_det = requests.get(detail_url, headers=headers, timeout=15)
            if r_det.status_code == 429:
                poly_str = act.get("map", {}).get("summary_polyline")
            else:
                details = r_det.json()
                poly_str = details.get("map", {}).get("polyline") or details.get("map", {}).get("summary_polyline")
        except:
            poly_str = act.get("map", {}).get("summary_polyline")

        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO activities 
            (id, date, name, sport_type, distance_m, moving_time_sec, polyline,
             start_lat, start_lon, end_lat, end_lon, location_city, location_state, raw_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            act["id"], date_str, act.get("name"),
            act.get("sport_type") or act.get("type"),
            act.get("distance"), act.get("moving_time"), poly_str,
            act.get("start_latitude"), act.get("start_longitude"),
            act.get("end_latitude"), act.get("end_longitude"),
            act.get("location_city"), act.get("location_state"),
            json.dumps(act), datetime.now().isoformat()
        ))
        saved += 1

    conn.commit()
    conn.close()

    df = pd.read_sql_query("SELECT * FROM activities ORDER BY date DESC", sqlite3.connect(db_file))
    df.to_csv(csv_file, index=False)
    print(f"✅ Exported {len(df)} activities to {csv_file}")
    print(f"✅ Created {db_file} with {saved} qualifying activities.")
    return db_file

# ======================== MAIN ========================
if __name__ == "__main__":
    args = [arg for arg in sys.argv[1:] if not arg.startswith("--") and arg not in [str(buffer_ft)]]
    if len(args) < 1:
        city_input = input("Enter city name (e.g. San Francisco): ").strip()
    else:
        city_input = " ".join(args).strip()

    if not city_input:
        print("❌ Please provide a city name.")
        sys.exit(1)

    safe_city = sanitize_name(city_input)
    streets_db = f"{safe_city}_completion.db"
    grey_map_file = f"{safe_city}_streets_map_inventory.html"

    print(f"\n🚀 Starting CityCompletion for: {city_input} (GPS Buffer: {buffer_ft} ft)")

    # === 1. Grey reference map (robust check) ===
    print("Step 1/5: Checking/creating smooth grey reference map...")
    
    db_needs_rebuild = True
    if os.path.exists(streets_db):
        try:
            conn = sqlite3.connect(streets_db)
            columns = [col[1] for col in conn.execute("PRAGMA table_info(streets)").fetchall()]
            if 'geom_json_v2' in columns: # Enforcing a rebuild for the dual-carriageway cleanup
                db_needs_rebuild = False
            conn.close()
        except:
            pass

    map_files = [f for f in os.listdir('.') if f.endswith("_streets_map_inventory.html") and safe_city in f]
    map_exists = len(map_files) > 0

    if map_exists and not db_needs_rebuild:
        grey_map_file = map_files[0]
        print(f"   ✅ Using existing smooth grey map and valid DB schema: {grey_map_file}")
    else:
        print("   → Creating/Upgrading detailed grey map and database (this takes a moment)...")
        if os.path.exists(streets_db) and db_needs_rebuild:
            os.remove(streets_db)
            print(f"   🗑️  Deleted old incompatible database: {streets_db}")
            
        try:
            place_query = f"{city_input}, California, USA"
            G = ox.graph_from_place(place_query, network_type="drive", simplify=True, truncate_by_edge=True)
            edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
            edges = edges[~edges['highway'].astype(str).str.contains('motorway|trunk', na=False)]

            # --- DUAL CARRIAGEWAY CLEANUP (Divided Highways) ---
            print("   → Consolidating divided roads into single lines...")
            edges_proj = edges.to_crs(epsg=3857)
            edges_proj['norm_name'] = edges_proj['name'].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else (str(x) if pd.notna(x) else "")
            )
            
            kept_indices = []
            for name, group in edges_proj.groupby('norm_name'):
                if not name:
                    kept_indices.extend(group.index)
                    continue
                
                kept_geoms = []
                for idx, row in group.iterrows():
                    geom = row.geometry
                    is_duplicate = False
                    for k in kept_geoms:
                        # If a line of the same name falls mostly inside a 20m buffer of another, 
                        # it's a parallel divided highway lane. Drop it.
                        k_buffer = k.buffer(20)
                        overlap = geom.intersection(k_buffer)
                        if overlap.length / geom.length > 0.6:
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        kept_geoms.append(geom)
                        kept_indices.append(idx)
                        
            edges = edges.loc[kept_indices]
            print(f"   ✅ Consolidated map down to {len(edges)} distinct street edges.")
            # ---------------------------------------------------

            conn = sqlite3.connect(streets_db)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS streets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT,
                    street_name TEXT,
                    geom_json_v2 TEXT,
                    length_feet REAL,
                    osm_way_ids TEXT,
                    timestamp TEXT
                )
            ''')
            conn.commit()

            cursor = conn.cursor()
            inserted = 0
            
            for _, edge in edges.iterrows():
                try:
                    coords_list = list(edge.geometry.coords)
                    coords_json = json.dumps(coords_list)
                    length_feet = float(edge.get("length", 0)) * 3.28084

                    street_name = edge.get("name")
                    if isinstance(street_name, list):
                        street_name = ", ".join(filter(None, street_name))
                    if not street_name:
                        street_name = "Unnamed street"

                    osm_id_val = edge.get("osmid")
                    osm_id_val = ", ".join(map(str, osm_id_val)) if isinstance(osm_id_val, list) else str(osm_id_val)

                    cursor.execute('''
                        INSERT INTO streets 
                        (city, street_name, geom_json_v2, length_feet, osm_way_ids, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (city_input, street_name, coords_json, length_feet, osm_id_val, datetime.now().isoformat()))
                    inserted += 1
                except:
                    continue

            conn.commit()
            conn.close()

            m = folium.Map(location=[edges.geometry.centroid.y.mean(), edges.geometry.centroid.x.mean()], zoom_start=13, tiles="CartoDB positron")
            for _, edge in edges.iterrows():
                coords = [(lat, lon) for lon, lat in edge.geometry.coords]
                street_name = edge.get("name")
                street_name = ", ".join(filter(None, street_name)) if isinstance(street_name, list) else (street_name or "Unnamed")
                folium.PolyLine(coords, color="#888888", weight=2.2, opacity=0.75, popup=f"<b>{street_name}</b>").add_to(m)

            title_html = f'<div style="position:fixed;top:10px;left:50px;background:white;padding:10px 15px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.3);z-index:1000;"><h4 style="margin:0;color:#555555;">{city_input} Street Network</h4><small>Smooth detailed grey reference map</small></div>'
            m.get_root().html.add_child(folium.Element(title_html))
            m.save(grey_map_file)
            print(f"   ✅ Created smooth grey map: {grey_map_file}")

        except Exception as e:
            print(f"❌ Failed to create grey map: {e}")
            sys.exit(1)

    # === 2. Strava Activities ===
    print("Step 2/5: Handling dated Strava activities...")
    if "--update" in sys.argv or not any(f.startswith("strava_activities_") and f.endswith(".db") for f in os.listdir('.')):
        strava_db = create_strava_activities_db()
    else:
        strava_db = max([f for f in os.listdir('.') if f.startswith("strava_activities_") and f.endswith(".db")])
        print(f"   ✅ Using existing dated DB: {strava_db}")

    # === 3. Raw GPS Map ===
    print("Step 3/5: Generating Raw GPS overlay map...")
    conn = sqlite3.connect(streets_db)
    city_streets = conn.execute("SELECT geom_json_v2, street_name FROM streets").fetchall()
    conn.close()

    conn_routes = sqlite3.connect(strava_db)
    user_routes = conn_routes.execute("SELECT polyline, name, sport_type FROM activities WHERE polyline IS NOT NULL").fetchall()
    conn_routes.close()

    m_raw = folium.Map(location=[37.7749, -122.4194], zoom_start=13, tiles="CartoDB positron")

    for row in city_streets:
        try:
            coords_list = json.loads(row[0])
            folium_coords = [(lat, lon) for lon, lat in coords_list]
            folium.PolyLine(folium_coords, color="#888888", weight=2.0, opacity=0.75).add_to(m_raw)
        except:
            continue

    for poly_str, name, sport in user_routes:
        try:
            points = polyline.decode(poly_str)
            if len(points) >= 2:
                folium.PolyLine(points, color="#FC4C02", weight=3.5, opacity=0.85, popup=f"<b>{name}</b>").add_to(m_raw)
        except:
            continue

    m_raw.get_root().html.add_child(folium.Element(f'''
    <div style="position:fixed;top:10px;left:50px;background:white;padding:12px 18px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.4);z-index:1000;">
        <h4 style="margin:0;color:#FC4C02;">{city_input} — Raw Strava GPS Routes</h4>
        <small>Grey = city streets | Orange = actual GPS tracks</small>
    </div>'''))
    m_raw.save(f"{safe_city}_raw_gps.html")
    print(f"   ✅ Raw GPS map saved: {safe_city}_raw_gps.html")

    # === 4. Completion Map (PRECISE math method) ===
    print("Step 4/5: Generating Completion map (orange = fully traversed)...")
    streets_data = []
    for row in city_streets:
        try:
            coords_list = json.loads(row[0])
            geom = LineString(coords_list)
            streets_data.append({'geometry': geom, 'name': row[1]})
        except:
            continue

    city_gdf = gpd.GeoDataFrame(streets_data, crs="EPSG:4326")

    route_lines = []
    for poly_str, _, _ in user_routes:
        try:
            points = polyline.decode(poly_str)
            if len(points) >= 2:
                route_lines.append(LineString([(lon, lat) for lat, lon in points]))
        except:
            continue

    if route_lines:
        routes_gdf = gpd.GeoDataFrame(geometry=route_lines, crs="EPSG:4326")
        city_proj = city_gdf.to_crs(epsg=3857)
        routes_proj = routes_gdf.to_crs(epsg=3857)

        # Use the configured buffer size (converted to meters)
        routes_buffered = routes_proj.buffer(buffer_m)
        user_area = routes_buffered.unary_union
        user_area_gdf = gpd.GeoDataFrame({'geometry': [user_area]}, crs=3857)

        candidates = gpd.sjoin(city_proj, user_area_gdf, how='inner', predicate='intersects')
        candidate_indices = candidates.index.unique()

        city_proj['coverage_ratio'] = 0.0
        
        if len(candidate_indices) > 0:
            candidates_geom = city_proj.loc[candidate_indices, 'geometry']
            overlap_geoms = candidates_geom.intersection(user_area)
            city_proj.loc[candidate_indices, 'coverage_ratio'] = overlap_geoms.length / candidates_geom.length

        city_proj['covered'] = city_proj['coverage_ratio'] >= 0.75
        city_gdf['covered'] = city_proj['covered']
        city_gdf['coverage_ratio'] = city_proj['coverage_ratio']

        covered_count = int(city_gdf['covered'].sum())
    else:
        city_gdf['covered'] = False
        covered_count = 0

    print(f"   Coverage calculation complete ({covered_count} streets fully covered)")

    m_comp = folium.Map(location=[37.7749, -122.4194], zoom_start=13, tiles="CartoDB positron")

    print("   Building map (this step takes the longest)...")
    total_streets = len(city_gdf)
    progress_interval = max(1, total_streets // 10)

    for i, (_, row) in enumerate(city_gdf.iterrows()):
        if i % progress_interval == 0 or i == total_streets - 1:
            percent_done = int((i / total_streets) * 100)
            print(f"      Rendering map: {percent_done}% complete ({i}/{total_streets} segments added)", end="\r")

        coords = [(lat, lon) for lon, lat in row.geometry.coords]
        color = "#FC4C02" if row['covered'] else "#888888"
        weight = 4.5 if row['covered'] else 1.5
        
        percent_str = f"{(row.get('coverage_ratio', 0) * 100):.1f}%" if 'coverage_ratio' in row else "0%"
        popup_text = f"<b>{row['name']}</b><br>Coverage: {percent_str}"
        
        folium.PolyLine(coords, color=color, weight=weight, opacity=0.9, popup=popup_text).add_to(m_comp)

    print("\n      Rendering map: 100% complete!                            ")

    # --- TOP 10 LONGEST MISSING STREETS ---
    print("\n   [5/5] Top 10 Longest Uncompleted Streets:")
    uncovered_gdf = city_gdf[~city_gdf['covered']].copy()
    uncovered_proj = uncovered_gdf.to_crs(epsg=3857)
    uncovered_gdf['length_ft'] = uncovered_proj.geometry.length * 3.28084
    
    # Group by street name to combine total length
    top_uncovered = uncovered_gdf.groupby('name')['length_ft'].sum().reset_index()
    top_uncovered = top_uncovered.sort_values(by='length_ft', ascending=False).head(10)
    
    for i, (_, row) in enumerate(top_uncovered.iterrows(), 1):
        name = row['name'] if row['name'] else 'Unnamed'
        print(f"      {i}. {name} - {row['length_ft']:,.0f} ft")
    # --------------------------------------

    covered_percentage = round((covered_count / len(city_streets)) * 100, 1) if city_streets else 0

    m_comp.get_root().html.add_child(folium.Element(f'''
    <div style="position:fixed;top:10px;left:50px;background:white;padding:12px 18px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.4);z-index:1000;">
        <h4 style="margin:0;color:#FC4C02;">{city_input} Street Completion</h4>
        <small>Fully Covered (>= 75%): <strong>{covered_percentage}%</strong> ({covered_count}/{len(city_streets)} blocks)</small><br>
        <small style="color:#777;">GPS Drift Buffer: {buffer_ft} ft</small>
    </div>'''))
    m_comp.save(f"{safe_city}_completion.html")
    print(f"\n   ✅ Completion map saved: {safe_city}_completion.html")
    print(f"🎉 ALL DONE! ({covered_percentage}% Complete)\n")

    # === INTERACTIVE DEBUGGER TOOL ===
    if "--debug" in sys.argv:
        print("="*60)
        print("🕵️  STREET & ACTIVITY DEBUGGER LAUNCHED")
        print("="*60)
        
        conn_str = sqlite3.connect(streets_db)
        df_streets = pd.read_sql_query("SELECT street_name, geom_json_v2 FROM streets", conn_str)
        conn_str.close()
        
        conn_act = sqlite3.connect(strava_db)
        df_acts = pd.read_sql_query("SELECT name, polyline FROM activities WHERE polyline IS NOT NULL", conn_act)
        conn_act.close()

        while True:
            try:
                print("-" * 60)
                act_query = input("Enter Activity Name (or press Enter to quit): ").strip()
                if not act_query: break
                
                street_query = input("Enter Street Name: ").strip()
                if not street_query: continue
                
                act_matches = df_acts[df_acts['name'].str.contains(act_query, case=False, na=False)]
                if act_matches.empty:
                    print("   ❌ Activity not found.")
                    continue
                act_row = act_matches.iloc[0]
                print(f"   ✅ Found Activity: '{act_row['name']}'")
                
                street_matches = df_streets[df_streets['street_name'].str.contains(street_query, case=False, na=False)]
                if street_matches.empty:
                    print("   ❌ Street not found.")
                    continue
                print(f"   ✅ Found {len(street_matches)} segments for '{street_query}'")
                
                act_line = LineString([(lon, lat) for lat, lon in polyline.decode(act_row['polyline'])])
                act_gdf = gpd.GeoDataFrame({'geometry': [act_line]}, crs="EPSG:4326").to_crs(epsg=3857)
                act_buffer = act_gdf.geometry.iloc[0].buffer(buffer_m)
                
                street_geoms = [LineString(json.loads(g)) for g in street_matches['geom_json_v2']]
                street_gdf = gpd.GeoDataFrame({'name': street_matches['street_name'], 'geometry': street_geoms}, crs="EPSG:4326").to_crs(epsg=3857)
                
                total_len = 0
                covered_len = 0
                
                for idx, row in street_gdf.iterrows():
                    geom = row.geometry
                    total_len += geom.length
                    overlap = geom.intersection(act_buffer)
                    covered_len += overlap.length
                
                coverage_ratio = covered_len / total_len if total_len > 0 else 0
                print(f"\n   📊 Results for {street_query} on this run:")
                print(f"      Total Block Length : {total_len * 3.28084:,.0f} ft")
                print(f"      Covered by GPS     : {covered_len * 3.28084:,.0f} ft")
                print(f"      Coverage %         : {(coverage_ratio * 100):.1f}%")
                if coverage_ratio >= 0.75:
                    print("      🟢 Status: FULLY COVERED (>= 75%)")
                else:
                    print("      🔴 Status: INCOMPLETE (< 75%)")
            except Exception as e:
                print(f"   ⚠️ Error during debug: {e}")