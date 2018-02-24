# -*- coding:utf-8 -*-

import os
import sys

CMD_PREFIX = '>> '
RETURN_PREFIX = '#RETURN'
TODO_PREFIX = '#TODO'
COMMENT_PREFIX = '#'
UNFINISHED_1_PREFIX = '\t'
UNFINISHED_2_PREFIX = '   '

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

class Matcher:
    def __init__(self, executor):
        self.executor = executor
        self.query = None
        self.outputs = None
        self.matches = []

    def on_line(self, line):
        if line.startswith(CMD_PREFIX):
            if self.outputs != None and not matched(self.outputs, self.matches):
                return False
            self.query = line[len(CMD_PREFIX):]
            self.outputs = self.executor.exe(self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.matches = []
        else:
            self.matches.append(line)
        return True

    def on_finish(self):
        if self.outputs != None and not matched(self.outputs, self.matches):
            return False
        return True

def parse_exe_match(path, executor):
    todos = []
    with open(path) as file:
        matcher = Matcher(executor)
        cached = None
        for origin in file:
            line = origin.strip()
            if line.startswith(RETURN_PREFIX):
                break
            if line.startswith(TODO_PREFIX):
                todos.append(line[len(TODO_PREFIX):].strip())
                continue
            if line.startswith(COMMENT_PREFIX) or len(line) == 0:
                continue
            if origin.startswith(UNFINISHED_1_PREFIX) or origin.startswith(UNFINISHED_2_PREFIX):
                if cached[-1] == ',':
                    cached += ' '
                cached += line
                continue
            if cached != None and not matcher.on_line(cached):
                return False, matcher, todos
            cached = line
        if (cached != None and not matcher.on_line(cached)) or not matcher.on_finish():
            return False, matcher, todos
        return True, matcher, todos

def main():
    if len(sys.argv) != 3:
        print 'usage: <bin> database-client-cmd test-file-path'
        sys.exit(1)

    dbc = sys.argv[1]
    path = sys.argv[2]

    matched, matcher, todos = parse_exe_match(path, Executor(dbc))

    def display(lines):
        if len(lines) == 0:
            print ' ' * 4 + '<nothing>'
        else:
            for it in lines:
                print ' ' * 4 + it

    if not matched:
        print '  Error:', matcher.query
        print '  Result:'
        display(matcher.outputs)
        print '  Expected:'
        display(matcher.matches)
        sys.exit(1)
    if len(todos) != 0:
        print '  TODO:'
        for it in todos:
            print ' ' * 4 + it

main()
