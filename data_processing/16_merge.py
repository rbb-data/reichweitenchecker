"""
Merge all result files into a single file.
"""

import json
import glob

IN_DIR = "data/with_google_maps_data"
OUT = "data/with_google_maps_data.json"


def main():
    files = glob.glob(IN_DIR + "/*.json")

    data = []
    for filename in files:
        with open(filename, "r", encoding="utf-8") as f:
            data.append(json.load(f))

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
