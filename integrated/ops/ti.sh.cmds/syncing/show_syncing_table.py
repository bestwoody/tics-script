import json
import urllib
import sys
import time

def error(msg):
    sys.stderr.write('[show_syncing_table.py] ' + msg + '\n')
    sys.exit(1)

def parse_table_ids(rules_str):
    if rules_str == "null":
        return []
    result = json.loads(rules_str)
    if not result:
        return []
    table_ids = []
    for item in result:
        if item["group_id"] != "tiflash" or item["role"] != "learner":
            continue
        parsed_item_id = item["id"].split('-')
        if parsed_item_id[0] != "table" or parsed_item_id[2] != "r":
            error("id format changed.")
        table_ids.append(parsed_item_id[1])
    return table_ids

def get_db_and_table(schema_str, db=""):
    if not schema_str or len(schema_str) == 0:
        return "", ""
    try:
        schema = json.loads(schema_str)
    except Exception, e:
        error(str(e) + ':' + schema_str)
        return "", ""
    if db != "" and schema["db_info"]["db_name"]["O"] != db:
        return "", ""
    return schema["db_info"]["db_name"]["O"], schema["table_info"]["name"]["O"]

def open_url(url):
    max_try_times = 100
    max_sleep = 8
    i = 0
    sleep_interval = 1
    while True:
        try:
            f = urllib.urlopen(url)
        except Exception as e:
            if i >= max_try_times:
                raise
            else:
                print "open " + rules_request_url + " failed"
                i += 1
            time.sleep(sleep_interval)
            if sleep_interval < max_sleep:
                sleep_interval *= 2
        else:
            return f

def run(pd_host, pd_port, tidb_host, tidb_port, target_db=""):
    rules_request_url = "http://" + pd_host + ":" + pd_port + "/pd/api/v1/config/rules/group/tiflash"
    f = open_url(rules_request_url)
    if f == '' or not f:
        return
    table_ids = parse_table_ids(f.read())
    tables = {}
    for table_id in table_ids:
        schema_request_rule = "http://" + tidb_host + ":" + tidb_port + "/db-table/" + table_id
        f = open_url(schema_request_rule)
        if f == '' or not f:
            return
        s = f.read()
        if s.find(' does not exist') >= 0:
            return
        db, table = get_db_and_table(s, target_db)
        if table != "":
            if db not in tables:
                tables[db] = []
            tables[db].append(table)
    for db in tables:
        print db
        for table in tables[db]:
            print '    ' + table

if __name__ == '__main__':
    if len(sys.argv) < 6:
        error('usage: <bin> pd_host pd_port tidb_host tidb_port [db]')

    pd_host = sys.argv[1]
    pd_port = sys.argv[2]
    tidb_host = sys.argv[3]
    tidb_port = sys.argv[4]
    db = ""
    if len(sys.argv) >= 6:
        db = sys.argv[5]
    run(pd_host, pd_port, tidb_host, tidb_port, db)
