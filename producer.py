import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

from config import (
    START_DATE, END_DATE, TRAIN_LIMIT, STREAM_DELAY_SECONDS,
    TRAINS_URL, STATIONS_URL, ELEVATION_URL,
    WEIGHTS, CACHE_DIR, TRAINS_FILE, STATIONS_FILE, ELEVATION_FILE,
)
from scoring import calculate_components, weighted_score, risk_band

Path(CACHE_DIR).mkdir(exist_ok=True)


def download_json(url, path):
    if os.path.exists(path):
        print(f"[producer] Using cached {path}", file=sys.stderr, flush=True)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    print(f"[producer] Downloading {url}", file=sys.stderr, flush=True)
    req = Request(url, headers={"User-Agent": "IndianRailwayBrakeAnalytics/1.0"})
    with urlopen(req, timeout=60) as r:
        data = json.loads(r.read().decode("utf-8"))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data


def parse_time(value):
    if not value:
        return None
    return datetime.strptime(value, "%H:%M:%S").time()


def minutes_from_origin(stop):
    t = parse_time(stop.get("arrivalTime") or stop.get("departureTime"))
    if not t:
        return None
    base = t.hour * 60 + t.minute + t.second / 60
    return (stop.get("journeyDay", 1) - 1) * 1440 + base


def trip_duration_hours(route):
    if len(route) < 2:
        return None
    start = minutes_from_origin(route[0])
    end = minutes_from_origin(route[-1])
    if start is None or end is None or end <= start:
        return None
    return (end - start) / 60.0


def running_dates(running_days):
    names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    current = START_DATE
    while current <= END_DATE:
        if running_days.get(names[current.weekday()], False):
            yield current
        current += timedelta(days=1)


def get_elevations(station_codes, station_map):
    # Load cache.
    elevations = {}
    if os.path.exists(ELEVATION_FILE):
        with open(ELEVATION_FILE, "r", encoding="utf-8") as f:
            elevations = json.load(f)

    missing = []
    for code in station_codes:
        if code in elevations:
            continue
        s = station_map.get(code)
        if s and s.get("coordinates"):
            lat = s["coordinates"].get("latitude")
            lon = s["coordinates"].get("longitude")
            if lat is not None and lon is not None:
                missing.append((code, lat, lon))

    # Open-Meteo supports batches up to 100 coordinates.
    for i in range(0, len(missing), 100):
        batch = missing[i:i + 100]
        lats = ",".join(str(x[1]) for x in batch)
        lons = ",".join(str(x[2]) for x in batch)
        url = f"{ELEVATION_URL}?latitude={lats}&longitude={lons}"
        print(f"[producer] Fetching elevation for {len(batch)} stations...", file=sys.stderr, flush=True)
        try:
            req = Request(url, headers={"User-Agent": "IndianRailwayBrakeAnalytics/1.0"})
            with urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode("utf-8"))
            vals = data.get("elevation", [])
            for item, elev in zip(batch, vals):
                elevations[item[0]] = elev
        except Exception as exc:
            print(f"[producer] Elevation request failed: {exc}", file=sys.stderr, flush=True)

    with open(ELEVATION_FILE, "w", encoding="utf-8") as f:
        json.dump(elevations, f, indent=2)
    return elevations


def route_metrics(train, station_map, elevations):
    route = train.get("completeOrderedRoute", [])
    distance = float(train.get("overallDistanceKm") or 0)
    duration = trip_duration_hours(route)
    speed = distance / duration if duration and duration > 0 else 0

    stops = max(0, len(route) - 2)
    elev_values = []
    for stop in route:
        code = stop.get("stationCode")
        if code in elevations:
            elev_values.append((stop, float(elevations[code])))

    ascent = 0.0
    descent = 0.0
    max_downhill_drop = 0.0
    max_elevation = None
    min_elevation = None
    downhill_segments = 0

    for (_, prev_e), (_, curr_e) in zip(elev_values, elev_values[1:]):
        delta = curr_e - prev_e
        if delta > 0:
            ascent += delta
        elif delta < 0:
            descent += -delta
            downhill_segments += 1
            max_downhill_drop = max(max_downhill_drop, -delta)

    if elev_values:
        all_e = [e for _, e in elev_values]
        max_elevation = max(all_e)
        min_elevation = min(all_e)

    return {
        "distance_km": round(distance, 2),
        "scheduled_duration_hours": round(duration, 2) if duration else None,
        "average_speed_kmph": round(speed, 2),
        "intermediate_stops": stops,
        "stations_with_elevation": len(elev_values),
        "total_ascent_m": round(ascent, 1),
        "total_descent_m": round(descent, 1),
        "max_elevation_m": round(max_elevation, 1) if max_elevation is not None else None,
        "min_elevation_m": round(min_elevation, 1) if min_elevation is not None else None,
        "downhill_segments": downhill_segments,
        "max_downhill_drop_m": round(max_downhill_drop, 1),
    }


def build_trips(trains, station_map, elevations):
    candidates = []
    for train in trains:
        route = train.get("completeOrderedRoute") or []
        if len(route) < 2:
            continue
        metrics = route_metrics(train, station_map, elevations)
        if metrics["distance_km"] <= 0 or not metrics["scheduled_duration_hours"]:
            continue
        for d in running_dates(train.get("runningDays", {})):
            candidates.append((d, train, metrics))
    candidates.sort(key=lambda x: (x[0], str(x[1].get("trainNumber", ""))))
    return candidates


def percentile_bounds(trips):
    keys = {
        "distance": "distance_km",
        "stops": "intermediate_stops",
        "ascent": "total_ascent_m",
        "descent": "total_descent_m",
        "speed": "average_speed_kmph",
    }
    bounds = {}
    for name, key in keys.items():
        vals = [float(t[2][key]) for t in trips if t[2][key] is not None]
        bounds[name] = (min(vals), max(vals)) if vals else (0.0, 1.0)
    return bounds


def main():
    print("[producer] Starting Indian Railway BRAKE-STRESS historical stream", file=sys.stderr, flush=True)
    print(f"[producer] Replay window: {START_DATE} -> {END_DATE}", file=sys.stderr, flush=True)
    print(f"[producer] Train limit: {TRAIN_LIMIT if TRAIN_LIMIT else 'ALL'}", file=sys.stderr, flush=True)

    stations = download_json(STATIONS_URL, STATIONS_FILE)
    trains = download_json(TRAINS_URL, TRAINS_FILE)

    if TRAIN_LIMIT:
        trains = trains[:TRAIN_LIMIT]

    station_map = {s.get("code"): s for s in stations if s.get("code")}
    route_codes = set()
    for t in trains:
        for stop in t.get("completeOrderedRoute", []):
            if stop.get("stationCode") in station_map:
                route_codes.add(stop["stationCode"])

    elevations = get_elevations(sorted(route_codes), station_map)
    trip_candidates = build_trips(trains, station_map, elevations)
    bounds = percentile_bounds(trip_candidates)

    # Cumulative exposure is accumulated per train as the chronological stream is emitted.
    cumulative = {}
    trip_counter = {}

    for trip_index, (trip_date, train, metrics) in enumerate(trip_candidates, start=1):
        number = str(train.get("trainNumber"))
        cumulative.setdefault(number, {"trips": 0, "distance_km": 0.0, "exposure_score": 0.0})
        trip_counter[number] = trip_counter.get(number, 0) + 1

        components = calculate_components(metrics, bounds)
        score = weighted_score(components, WEIGHTS)

        cumulative[number]["trips"] += 1
        cumulative[number]["distance_km"] += metrics["distance_km"]
        cumulative[number]["exposure_score"] += score

        event = {
            "event_type": "completed_scheduled_trip",
            "stream_sequence": trip_index,
            "trip_date": trip_date.isoformat(),
            "train_number": number,
            "train_name": train.get("trainName"),
            "train_type": train.get("type"),
            "origin": (train.get("source") or {}).get("name"),
            "destination": (train.get("destination") or {}).get("name"),
            "trip_number_for_train": trip_counter[number],
            **metrics,
            "score_components": {k: round(v, 2) for k, v in components.items()},
            "brake_stress_score_0_100": score,
            "risk_band": risk_band(score),
            "cumulative_trips_for_train": cumulative[number]["trips"],
            "cumulative_distance_km": round(cumulative[number]["distance_km"], 2),
            "cumulative_brake_stress_exposure": round(cumulative[number]["exposure_score"], 2),
            "model_note": "Terrain/operation proxy only; not measured brake pressure, temperature, pad wear, or failure probability.",
            "score_weights": WEIGHTS,
        }

        print(json.dumps(event, separators=(",", ":")), flush=True)
        time.sleep(STREAM_DELAY_SECONDS)

    print(f"[producer] Finished. Streamed {len(trip_candidates)} trip events.", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
