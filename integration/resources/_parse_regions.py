# -*- coding:utf-8 -*-

import sys
import json

def load():
    lines = ""
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if len(line) == 0:
            continue

        lines += line
    return lines

def parse(data):
    res = json.loads(data)
    for region in res["regions"]:
        print region["id"], region.has_key("approximate_keys") and region["approximate_keys"] or 0

def main():
    try:
        parse(load())
    except:
        pass

main()
