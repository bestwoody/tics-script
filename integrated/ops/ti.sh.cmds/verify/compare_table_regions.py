import json
import sys
import prettytable as pt

def error(msg):
    sys.stderr.write('[compare_table_regions.py] ' + msg + '\n')
    sys.exit(1)

def print_normal(compare_results):
    sorted_results = sorted(compare_results, key=lambda result: result["region_json"]["start_key"])

    tb = pt.PrettyTable()
    tb.field_names = ["region_id", "in_pd", "in_ch", "start_key", "end_key"]
    for field_name in tb.field_names:
        tb.align[field_name] = "r"

    for i in range(0, len(sorted_results)):
        compare_result = sorted_results[i]
        tb.add_row([compare_result["region_id"],
                    int(compare_result["in_pd"] == True),
                    int(compare_result["in_ch"] == True),
                    compare_result["region_json"]["start_key"],
                    compare_result["region_json"]["end_key"]])
    print(tb)

def should_merge(left, right):
    return left["end_key"] == right["start_key"] and left["in_pd"] == right["in_pd"] and left["in_ch"] == right["in_ch"]

def do_merge(sorted_results, i):
    left = sorted_results[i]
    right = sorted_results[i + 1]
    merged = {
        "compare_results": left["compare_results"] + right["compare_results"],
        "start_key": left["start_key"],
        "end_key": right["end_key"],
        "in_pd": left["in_pd"],
        "in_ch": left["in_ch"],
        "min_region_id": min(left["min_region_id"], right["min_region_id"]),
        "max_region_id": max(left["max_region_id"], right["max_region_id"])
    }
    tmp = sorted_results[: i]
    tmp.append(merged)
    return tmp + sorted_results[i + 2 :]

def merge(sorted_results):
    while True:
        should_return = True
        for i in range(0, len(sorted_results) - 1):
            l = sorted_results[i]
            r = sorted_results[i + 1]
            if should_merge(l, r):
                sorted_results = do_merge(sorted_results, i)
                should_return = False
                break
        if should_return:
            return sorted_results

def print_pretty(compare_results):
    wrapped_results = []
    for i in range(0, len(compare_results)):
        compare_result = compare_results[i]
        region_id = compare_result["region_id"]
        region_json = compare_result["region_json"]
        wrapped_results.append({
            "compare_results": [compare_result],
            "start_key": region_json["start_key"],
            "end_key": region_json["end_key"],
            "in_pd": compare_result["in_pd"],
            "in_ch": compare_result["in_ch"],
            "min_region_id": int(region_id),
            "max_region_id": int(region_id)
        })

    sorted_results = sorted(wrapped_results, key=lambda result: result["start_key"])
    merged_results = merge(sorted_results)

    tb = pt.PrettyTable()
    tb.field_names = ["min_region", "max_region", "region_num", "in_pd", "in_ch", "start_key", "end_key"]
    for field_name in tb.field_names:
        tb.align[field_name] = "r"

    for i in range(0, len(merged_results)):
        merged_result = merged_results[i]
        tb.add_row([str(merged_result["min_region_id"]),
                    str(merged_result["max_region_id"]),
                    len(merged_result["compare_results"]),
                    int(merged_result["in_pd"] == True),
                    int(merged_result["in_ch"]== True),
                    merged_result["start_key"],
                    merged_result["end_key"]])
    print(tb)

def do_compare_table_regions(table_regions_all, table_regions_from_pd, table_regions_from_ch,
                             all_regions_from_pd, pretty):
    json_all_regions_from_pd = json.loads(all_regions_from_pd)
    json_regions = json_all_regions_from_pd["regions"]

    id_to_region_map = {}
    for i in range(0, len(json_regions)):
        json_region = json_regions[i]
        id = str(json_region["id"])
        id_to_region_map[id] = json_region
    
    compare_results = []
    for i in range(0, len(table_regions_all)):
        region_id = table_regions_all[i]
        compare_results.append({
            "region_id": region_id,
            "in_pd": region_id in table_regions_from_pd,
            "in_ch": region_id in table_regions_from_ch,
            "region_json": id_to_region_map[region_id]
        })

    if pretty == "true":
        print_pretty(compare_results)
    else:
        print_normal(compare_results)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        error(
            'usage: <bin> pretty separator')

    pretty = sys.argv[1]
    separator = sys.argv[2]

    input = ""
    for line in sys.stdin.readlines():
        if not line:
            break
        else:
            input = input + line

    args = input.split("****")
    table_regions_all = args[0]
    table_regions_from_pd = args[1]
    table_regions_from_ch = args[2]
    all_regions_from_pd = args[3]

    do_compare_table_regions(table_regions_all.split(), table_regions_from_pd.split(),
                             table_regions_from_ch.split(), all_regions_from_pd, pretty)
