# -*- coding:utf-8 -*-

import sys

def error(msg):
    sys.stderr.write('[print_root_pid.py] ' + msg + '\n')
    sys.exit(1)

def run():
    pids = set()
    ppids = set()

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if len(line) == 0:
            continue

        line = line[:-1].strip()
        if len(line) == 0:
            continue

        fields = line.split()
        if len(fields) != 2:
            error('bad "pid ppid" line: ' + line)

        pids.add(fields[0])
        ppids.add(fields[1])

    pids -= ppids

    if len(pids) == 1:
        print pids.pop()

if __name__ == '__main__':
    run()
