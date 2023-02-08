"""
For each dead station, find city centre stops that are nearby
"""

import os
import pandas as pd
import numpy as np
from geopy import distance
from urllib.parse import quote

DEAD_STATIONS_FILE = "data/dead_stations.csv"
CITY_FILE = "data/Public-Transport-2023-cities.csv"

OUT_DIR = "data/cities_nearby_dead_stations"
os.makedirs(OUT_DIR, exist_ok=True)


def main():
    df = pd.read_csv(DEAD_STATIONS_FILE).set_index("stop_name")
    dead_stations = [
        {"stop_name": index, "coords": (row["lat"], row["lon"])}
        for index, row in df.iterrows()
    ]

    df_city = pd.read_csv(CITY_FILE).set_index("stop_name")
    city_stations = [
        {"stop_name": index, "coords": (row["stop_lat"], row["stop_lon"])}
        for index, row in df_city.iterrows()
    ]

    # compute distances between all city main stations and given stops
    distances = np.zeros([len(dead_stations), len(city_stations)])
    for i, dead_station in enumerate(dead_stations):
        for j, city_station in enumerate(city_stations):
            distances[i, j] = distance.distance(
                dead_station["coords"], city_station["coords"]
            ).meters
        print(i + 1, "/", len(dead_stations))

    for dead_station_id in range(len(dead_stations)):
        dead_station = dead_stations[dead_station_id]
        dist_to_dead = distances[dead_station_id]

        df_nearby = df_city.copy(deep=True)
        df_nearby["distance"] = dist_to_dead
        df_nearby = df_nearby.sort_values(by="distance")

        df_nearby.to_csv(
            OUT_DIR + "/" + quote(dead_station["stop_name"], safe="") + ".csv"
        )


if __name__ == "__main__":
    main()
