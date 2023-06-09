import json
import sys

def error(msg):
    sys.stderr.write('[parse_table_id.py] ' + msg + '\n')
    sys.exit(1)

def parse_table_id(regions_str):
    if regions_str == "null":
        return null
    result = json.loads(regions_str)
    if not result:
        return null
    return result["id"]

if __name__ == '__main__':
    regions_pd_str = ""
    for line in sys.stdin.readlines():
        if not line:
            break
        else:
            regions_pd_str = regions_pd_str + line

    table_id = parse_table_id(regions_pd_str)
    print table_id
