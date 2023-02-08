"""
Find stations that fail to reach any city centre within 1 hour
"""

import glob
import pandas as pd

import ujson as json


data_dir = "data/merged"
filenames = glob.glob(f"{data_dir}/*.json")
out = "data/dead_stations"

features = []
data = []

for filename in filenames:
    with open(filename, "r", encoding="utf-8") as f:
        content = json.load(f)

    if (
        len(content["travelTimes"]["Werktag"]["Tag"]) > 0
        or len(content["travelTimes"]["Werktag"]["Nacht"]) > 0
        or len(content["travelTimes"]["Samstag"]["Tag"]) > 0
        or len(content["travelTimes"]["Samstag"]["Nacht"]) > 0
        or len(content["travelTimes"]["Sonntag"]["Tag"]) > 0
        or len(content["travelTimes"]["Sonntag"]["Nacht"]) > 0
    ):
        continue

    data.append(
        {
            "stop_id": content["stopInfo"]["id"],
            "stop_name": content["stopInfo"]["name"],
            "municipality": content["stopInfo"]["municipality"],
            "lat": content["stopInfo"]["coord"][0],
            "lon": content["stopInfo"]["coord"][1],
        }
    )

    features.append(
        {
            "type": "Feature",
            "properties": {
                "stop_id": content["stopInfo"]["id"],
                "stop_name": content["stopInfo"]["name"],
                "municipality": content["stopInfo"]["municipality"],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    content["stopInfo"]["coord"][1],
                    content["stopInfo"]["coord"][0],
                ],
            },
        }
    )

df = pd.DataFrame(data)
df.to_csv(f"{out}.csv", index=False)

with open(f"{out}.geojson", "w") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f)

print("# dead stations", len(df))
