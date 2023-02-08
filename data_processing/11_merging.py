"""
Merges the data from the three days into one file per stop.
"""


from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote
import os

import ujson as json


DATA_DIR = Path("data")

file_stops = DATA_DIR / "stops_with_coords.json"

dir_monday_day = DATA_DIR / "travel_times_proc_wednesday_day_combine"
dir_saturday_day = DATA_DIR / "travel_times_proc_saturday_day_combine"
dir_sunday_day = DATA_DIR / "travel_times_proc_sunday_day_combine"

dir_monday_night = DATA_DIR / "travel_times_proc_wednesday_night_combine"
dir_saturday_night = DATA_DIR / "travel_times_proc_saturday_night_combine"
dir_sunday_night = DATA_DIR / "travel_times_proc_sunday_night_combine"

target_path = DATA_DIR / "merged"
target_path.mkdir(exist_ok=True)

with open(file_stops, "r", encoding="utf-8") as f:
    stops = json.load(f)


def process_stop(stop_name, municipality, lat, lon):
    print(".", end="", flush=True)
    stop_name_enc = quote(stop_name, safe="")
    _filename = f"{stop_name_enc}.json"

    stop_info = None
    travel_times = {
        "Werktag": {"Tag": [], "Nacht": []},
        "Samstag": {"Tag": [], "Nacht": []},
        "Sonntag": {"Tag": [], "Nacht": []},
    }

    for label_day, label_time, filename in [
        ("Werktag", "Tag", dir_monday_day / _filename),
        ("Werktag", "Nacht", dir_monday_night / _filename),
        ("Samstag", "Tag", dir_saturday_day / _filename),
        ("Samstag", "Nacht", dir_saturday_night / _filename),
        ("Sonntag", "Tag", dir_sunday_day / _filename),
        ("Sonntag", "Nacht", dir_sunday_night / _filename),
    ]:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                content = json.load(f)

            if stop_info is None:
                stop_info = content["stop_info"]
                stop_info["municipality"] = municipality

            travel_times[label_day][label_time] = content["destinations"]

    if stop_info is None:
        stop_info = {
            "name": stop_name,
            "municipality": municipality,
            "coord": [lat, lon],
        }

    merged = {
        "stopInfo": stop_info,
        "travelTimes": travel_times,
    }

    with open(target_path / f"{stop_name_enc}.json", "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)


executor = ThreadPoolExecutor(max_workers=32)
futs = []
for stop_name, municipality, lat, lon in stops:
    futs.append(executor.submit(process_stop, stop_name, municipality, lat, lon))


for fut in futs:
    fut.result()

executor.shutdown(wait=True)
