# -*- coding:utf-8 -*-

import os
import sys

class Executor:
    def __init__(self, dbc):
        self.dbc = dbc
    def exe(self, cmd):
        return os.popen(self.dbc + ' "' + cmd + '" 2>&1').readlines()

def parse_table_parts(lines):
    parts = set()
    curr = []
    for line in lines:
        if line.startswith('┌'):
            if len(curr) != 0:
                parts.add('\n'.join(curr))
                curr = []
        curr.append(line)
    if len(curr) != 0:
        parts.add('\n'.join(curr))
    return parts

def matched(outputs, matches):
    if len(outputs) != len(matches):
        return False
    if len(outputs) == 0:
        return True
    a = parse_table_parts(outputs)
    b = parse_table_parts(matches)
    return a == b

def parse_exe_match(path, executor):
    CMD_PREFIX = '>> '
    with open(path) as f:
        query = None
        outputs = None
        matches = []
        for line in f:
            line = line[:-1].strip()
            if line.startswith(CMD_PREFIX):
                if outputs != None and not matched(outputs, matches):
                    return False, query, outputs, matches
                query = line[len(CMD_PREFIX):]
                outputs = executor.exe(query)
                outputs = map(lambda x: x.strip(), outputs)
                outputs = filter(lambda x: len(x) != 0, outputs)
                matches = []
            elif len(line) > 0:
                matches.append(line)
        if outputs != None and not matched(outputs, matches):
            return False, query, outputs, matches
    return True, query, outputs, matches

def main():
    if len(sys.argv) != 3:
        print 'usage: <bin> database-client-cmd test-file-path'
        sys.exit(1)

    dbc = sys.argv[1]
    path = sys.argv[2]

    matched, query, outputs, matches = parse_exe_match(path, Executor(dbc))

    if not matched:
        print '  Error:', query
        print '  Result:'
        for it in outputs:
            print ' ' * 4, it
        print '  Expected:'
        for it in matches:
            print ' ' * 4, it
        sys.exit(1)

main()
