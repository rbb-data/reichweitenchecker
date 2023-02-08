"""
Find journeys for dead stop to city centres using data from https://www.vbb.de/
"""

import os
import pandas as pd
from urllib.parse import quote
import re
import requests
from datetime import datetime
import json
from time import sleep
from pathlib import Path
import sys
import glob

IN_DIR = "data/merged"
DATA_DIR = "data/cities_nearby_dead_stations"

VBB_ENDPOINT = "https://v5.vbb.transport.rest/journeys"

target_dir = Path("data/with_vbb_data")
target_dir.mkdir(exist_ok=True)

requested_ids = {}


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
    day = {"name": "Samstag", "date": "2023-02-04"}
else:
    day = {"name": "Sonntag", "date": "2023-02-12"}

if given_time == "day":
    time = {"name": "Tag", "start": "08:00:00", "end": "20:00:00"}
else:
    time = {"name": "Nacht", "start": "20:00:00", "end": "23:59:59"}


def get_vbb_id(stop_id):
    if not stop_id.startswith("de"):
        return stop_id
    else:
        match = re.search(r"de:\d+:(\d+).*", stop_id)
        if match:
            return match.group(1)
    return None


def find_valid_stop_id(stop_id, stop_name):
    if stop_name in requested_ids:
        return requested_ids[stop_name]

    # request stop with the given id
    sleep(1)
    response = requests.get(
        f"https://v5.vbb.transport.rest/stops/{stop_id}",
    )

    # if there is a response, the given stop id should be valid
    if response.ok:
        return stop_id

    # if not, query VBBs database using the stop name
    sleep(1)
    response = requests.get(
        "https://v5.vbb.transport.rest/stations",
        params={
            "query": stop_name,
        },
    )

    if response.ok:
        data = response.json()

        if len(data) == 0:
            return None

        # sort hits by their score and extract the hit with the best score
        hits = sorted(list(data.values()), key=lambda d: d["score"], reverse=True)
        hit = hits[0]

        print("found station", stop_name, hit["name"])

        requested_ids[stop_name] = hit["id"]
        return hit["id"]

    # no stop id found for the given stop
    return None


def main():
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

        # get file with nearby stations for that stop
        filename_nearby = DATA_DIR + "/" + stop_name_enc + ".csv"

        # if there is no file with nearby stations, the stop is not dead,
        # write data to the target directory and continue
        if not os.path.exists(filename_nearby):
            with open(target_dir / f"{stop_name_enc}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            continue

        # get city centre stations closest to the current stop
        df_nearby = pd.read_csv(filename_nearby).sort_values(by="distance").iloc[:5]
        df_nearby["vbb_id"] = df_nearby["stop_id"].apply(get_vbb_id)

        if df_nearby["vbb_id"].isnull().sum() > 0:
            print(df_nearby[["stop_id", "vbb_id"]])

        # once 90 requests have been sent, sleep for a minute and reset the counter
        # (the limit is 100 requests per minute)
        if request_counter > 90:
            print("sleeping...", file=sys.stderr)
            sleep(60)
            request_counter = 0

        dead_stop_id = get_vbb_id(station_id)
        if dead_stop_id is None:
            print("No VBB id could be extracted", station_id)
            continue

        departure = datetime.fromisoformat(
            day["date"] + "T" + time["start"] + "+01:00"
        ).timestamp()

        # request journeys from the current stop to close city centres
        for city_row in df_nearby.itertuples():
            try:
                city_id = city_row.vbb_id

                print()
                print(index + 1, "/", n_files, file=sys.stderr)
                print(index + 1, "/", n_files, station_name, "->", city_row.stop_name)

                # request journeys
                sleep(1)
                response = requests.get(
                    VBB_ENDPOINT,
                    params={
                        "from": dead_stop_id,
                        "to": city_id,
                        "departure": int(departure),
                        "results": 1,
                        "transfers": 3,
                        "startWithWalking": False,
                        "subStops": False,
                        "entrances": False,
                        "remarks": False,
                    },
                )
                request_counter += 1
                d = response.json()

                if not response.ok:
                    print(
                        "Response not ok",
                        response.url,
                        response.status_code,
                        d["message"],
                    )

                    # attempt to request valid stop ids
                    # if the given stop ids weren't found
                    if d["message"] == "location/stop not found":
                        dead_stop_id = find_valid_stop_id(dead_stop_id, station_name)
                        city_id = find_valid_stop_id(city_id, city_row.stop_name)
                        request_counter += 4

                        if dead_stop_id is None:
                            print("Could not find stop id", station_name)
                            continue
                        if city_id is None:
                            print("Could not find stop id", city_row.stop_name)
                            continue

                        # retry computing journeys with updated stop ids
                        print("Retry...")
                        sleep(1)
                        response = requests.get(
                            VBB_ENDPOINT,
                            params={
                                "from": dead_stop_id,
                                "to": city_id,
                                "departure": int(departure),
                                "results": 1,
                                "transfers": 3,
                                "startWithWalking": False,
                                "subStops": False,
                                "entrances": False,
                                "remarks": False,
                            },
                        )
                        request_counter += 1
                        d = response.json()
                        print(response.url)

                        if not response.ok:
                            print(
                                "Response not ok",
                                response.url,
                                response.status_code,
                                d["message"],
                            )
                            continue
                    else:
                        continue

                if len(d["journeys"]) == 0:
                    print("No journeys found", response.url)
                    continue

                legs = d["journeys"][0]["legs"]

                if len(legs) == 0:
                    print("No legs", response.url)
                    continue

                start_time = datetime.fromisoformat(legs[0]["plannedDeparture"])
                end_time = datetime.fromisoformat(legs[-1]["plannedArrival"])

                d1 = datetime.fromisoformat(f"{day['date']}T{time['start']}+01:00")
                d2 = datetime.fromisoformat(f"{day['date']}T{time['end']}+01:00")

                # check if start time is within given time range
                if start_time < d1 or start_time > d2:
                    print("Start time not in given range", start_time, response.url)
                    continue

                # check if the destination was reached within an hour
                duration = (end_time - start_time).total_seconds()
                if duration > 60 * 60:
                    print("Duration", end_time - start_time, response.url)
                    continue

                # exclude walking legs to find the correct number of transitions
                legs_without_walking = [
                    l for l in legs if not ("walking" in l and l["walking"])
                ]

                if len(legs_without_walking) == 0:
                    print("No non-walking legs", response.url)
                    continue

                data["travelTimes"][day["name"]][time["name"]].append(
                    {
                        "id": city_row.stop_id,
                        "name": city_row.stop_name,
                        "time": duration,
                        "trans": len(legs_without_walking) - 1,
                        "coords": [city_row.stop_lat, city_row.stop_lon],
                    }
                )

            except Exception as e:
                print("Unknown error", str(e))

        with open(target_dir / f"{stop_name_enc}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
