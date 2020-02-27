import json
import sys

if __name__ == '__main__':
    input = ""
    for line in sys.stdin.readlines():
        if not line:
            break
        else:
            input = input + line

    all_regions_from_pd = input

    json_all_regions_from_pd = json.loads(all_regions_from_pd)
    json_regions = json_all_regions_from_pd["regions"]

    id_to_region_map = {}
    for i in range(0, len(json_regions)):
        json_region = json_regions[i]
        id = str(json_region["id"])
        print(id)
