"""
Find journeys for dead stop to city centres using data from Google Maps

IMPORTANT: Running this script will cost you money (1000 requests cost $5).
Add the variable API_KEY to secrets.py to run this script.
"""

import os
import pandas as pd
from urllib.parse import quote
from datetime import datetime
import json
from time import sleep
from pathlib import Path
import sys
import glob
import requests

from secrets import API_KEY

IN_DIR = "data/with_vbb_data"
DATA_DIR = "data/cities_nearby_dead_stations"

DIRECTIONS_ENDPOINT = "https://maps.googleapis.com/maps/api/directions/json"

target_dir = Path("data/with_google_maps_data")
target_dir.mkdir(exist_ok=True)


def usage():
    print(
        f"Usage: python {sys.argv[0]} " + "{wednesday,saturday,sunday} {day,night}",
        file=sys.stderr,
    )


if len(sys.argv) < 3:
    print("Arguments missing", end="\n\n", file=sys.stderr)
    usage()
    sys.exit(1)

given_day = sys.argv[1]
given_time = sys.argv[2]

assert given_day in ["wednesday", "saturday", "sunday"]
assert given_time in ["day", "night"]

if given_day == "wednesday":
    day = {"name": "Werktag", "date": "2023-02-08"}
elif given_day == "saturday":
    day = {"name": "Samstag", "date": "2023-02-11"}
else:
    day = {"name": "Sonntag", "date": "2023-02-12"}

if given_time == "day":
    time = {"name": "Tag", "start": "08:00:00", "end": "20:00:00"}
else:
    time = {"name": "Nacht", "start": "20:00:00", "end": "23:59:59"}


def has_journey(data):
    return (
        len(data["travelTimes"]["Werktag"]["Tag"]) > 0
        or len(data["travelTimes"]["Werktag"]["Nacht"]) > 0
        or len(data["travelTimes"]["Samstag"]["Tag"]) > 0
        or len(data["travelTimes"]["Samstag"]["Nacht"]) > 0
        or len(data["travelTimes"]["Sonntag"]["Tag"]) > 0
        or len(data["travelTimes"]["Sonntag"]["Nacht"]) > 0
    )


def is_dead(stop_name_enc):
    with open(IN_DIR + "/" + stop_name_enc + ".json", "r", encoding="utf-8") as f:
        data = json.load(f)

    return not has_journey(data)


def main():
    total_n_requests = 0

    files = glob.glob(IN_DIR + "/*.json")
    n_files = len(files)

    # this script is run multiple times, so we add to the target directory
    # instead of overwriting it
    result_files = list(target_dir.glob("*.json"))

    request_counter = 0
    for index, filename in enumerate(files):
        # if a file fot the given stop exists within the target directory, use that one
        basename = os.path.basename(filename)
        if target_dir / basename in result_files:
            fn = target_dir / basename
        else:
            fn = filename

        # read data
        with open(fn, "r", encoding="utf-8") as f:
            data = json.load(f)
        station_id = data["stopInfo"]["id"]
        station_name = data["stopInfo"]["name"]
        stop_name_enc = quote(station_name, safe="")
        stop_coords = data["stopInfo"]["coord"]

        # get file with nearby stations for that stop
        filename_nearby = DATA_DIR + "/" + stop_name_enc + ".csv"

        # if not dead, write data to the target directory and continue
        if not is_dead(stop_name_enc):
            with open(target_dir / f"{stop_name_enc}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            continue

        # get city centre stations closest to the current stop
        df_nearby = pd.read_csv(filename_nearby).sort_values(by="distance").iloc[:1]

        # once 100 requests have been sent, sleep for a minute and reset the counter
        if request_counter > 100:
            print("sleeping...", file=sys.stderr)
            sleep(60)
            request_counter = 0

        departure = datetime.fromisoformat(
            day["date"] + "T" + time["start"]
        ).timestamp()

        # request journeys from the current stop to close city centres
        for city_row in df_nearby.itertuples():
            try:
                city_coords = [city_row.stop_lat, city_row.stop_lon]

                print()
                print(index + 1, "/", n_files, file=sys.stderr)
                print(index + 1, "/", n_files, station_name, "->", city_row.stop_name)

                # request journeys
                sleep(1)
                response = requests.get(
                    DIRECTIONS_ENDPOINT,
                    params={
                        "origin": ",".join(map(str, stop_coords)),
                        "destination": ",".join(map(str, city_coords)),
                        "key": API_KEY,
                        "mode": "transit",  # transit, walking
                        "units": "metric",
                        "departure_time": int(departure),
                    },
                )
                request_counter += 1
                total_n_requests += 1
                d = response.json()

                print("total # of requests:", total_n_requests, file=sys.stderr)

                if not response.ok:
                    print(
                        "Response not ok",
                        response.url,
                        response.status_code,
                        d["message"],
                    )
                    continue

                if d["status"] != "OK":
                    print("Response not ok", response.url, d["status"])
                    continue

                if len(d["routes"]) == 0:
                    print("No journeys found", response.url)
                    continue

                legs = d["routes"][0]["legs"]

                if len(legs) == 0:
                    print("No legs", response.url)
                    continue

                leg = legs[0]
                steps = leg["steps"]

                # check if the destination is in walking distance
                if len(steps) == 1 and steps[0]["travel_mode"] == "WALKING":
                    duration = leg["duration"]["value"]
                    if duration > 60 * 60:
                        print("Duration", leg["duration"]["text"], response.url)
                        continue
                    data["travelTimes"][day["name"]][time["name"]].append(
                        {
                            "id": city_row.stop_id,
                            "name": city_row.stop_name,
                            "time": duration,
                            "trans": 0,
                            "coords": [city_row.stop_lat, city_row.stop_lon],
                            "walking": True,
                        }
                    )
                    continue

                start_time = datetime.fromtimestamp(leg["departure_time"]["value"])
                end_time = datetime.fromtimestamp(leg["arrival_time"]["value"])

                d1 = datetime.fromisoformat(f"{day['date']}T{time['start']}")
                d2 = datetime.fromisoformat(f"{day['date']}T{time['end']}")

                # check if start time is within given time range
                if start_time < d1 or start_time > d2:
                    print("Start time not in given range", start_time, response.url)
                    continue

                # check if the destination was reached within an hour
                duration = leg["duration"]["value"]
                if duration > 60 * 60:
                    print("Duration", leg["duration"]["text"], response.url)
                    continue

                # exclude walking legs to find the correct number of transitions
                steps_without_walking = [
                    s for s in steps if not s["travel_mode"] == "WALKING"
                ]

                if len(steps_without_walking) == 0:
                    print("No non-walking legs", response.url)
                    continue

                data["travelTimes"][day["name"]][time["name"]].append(
                    {
                        "id": city_row.stop_id,
                        "name": city_row.stop_name,
                        "time": duration,
                        "trans": len(steps_without_walking) - 1,
                        "coords": [city_row.stop_lat, city_row.stop_lon],
                    }
                )

            except Exception as e:
                print("Unknown error", str(e))

        with open(target_dir / f"{stop_name_enc}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
