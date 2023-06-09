import json
import sys

def error(msg):
    sys.stderr.write('[parse_table_regions.py] ' + msg + '\n')
    sys.exit(1)

def parse_table_regions(regions_str):
    if regions_str == "null":
        return []
    result = json.loads(regions_str)
    if not result:
        return []
    table_regions = []
    for item in result["record_regions"]:
        table_regions.append(item["region_id"])
    return table_regions

if __name__ == '__main__':
    regions_pd_str = ""
    for line in sys.stdin.readlines():
        if not line:
            break
        else:
            regions_pd_str = regions_pd_str + line

    table_regions = parse_table_regions(regions_pd_str)
    for table_region in table_regions:
        print table_region
