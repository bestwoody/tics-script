# -*- coding:utf-8 -*-

import os
import sys
import time

CMD_PREFIX = '>> '
CMD_PREFIX_ALTER = '=> '
CMD_PREFIX_TI_MYSQL = 'ti_mysql> '
CMD_PREFIX_TI_BEELINE = 'ti_beeline> '
RETURN_PREFIX = '#RETURN'
SLEEP_PREFIX = 'SLEEP '
TODO_PREFIX = '#TODO'
COMMENT_PREFIX = '#'
UNFINISHED_1_PREFIX = '\t'
UNFINISHED_2_PREFIX = '   '
WORD_PH = '{#WORD}'

class Executor:
    def __init__(self, dbc):
        self.dbc = dbc
    def exe(self, cmd):
        return os.popen((self.dbc + ' "' + cmd + '" 2>&1').strip()).readlines()

class OpsExecutor:
    def __init__(self, ti_sh_path, ti_file_path):
        self.ti_sh_path = ti_sh_path
        self.ti_file_path = ti_file_path
    def exe(self, cmd_type, cmd):
        return os.popen((self.ti_sh_path + ' "' + self.ti_file_path + '" "' + cmd_type + '" "' + cmd + '" 2>&1').strip()).readlines()

def parse_line(line):
    words = [w.strip() for w in line.split("│") if w.strip() != ""]
    return "@".join(words)

def parse_beeline_line(line):
    words = [w.strip() for w in line.split("|") if w.strip() != ""]
    return "@".join(words)

def parse_table_parts(lines, fuzz, beeline_cmd=False):
    parts = set()
    if beeline_cmd:
        if not fuzz:
            curr = []
            for line in lines:
                if line.find('|') == -1:
                    continue
                curr.append(parse_beeline_line(line))
            if len(curr) != 0:
                parts.add('\n'.join(curr))
        else:
            for line in lines:
                if line.find('|') == -1:
                    continue
                line = parse_beeline_line(line)
                while True:
                    if line not in parts:
                        break
                    line += '-extra'
                parts.add(line)
    else:
        if not fuzz:
            curr = []
            for line in lines:
                if line.startswith('┌'):
                    if len(curr) != 0:
                        parts.add('\n'.join(curr))
                        curr = []
                curr.append(parse_line(line))
            if len(curr) != 0:
                parts.add('\n'.join(curr))
        else:
            for line in lines:
                if not line.startswith('┌') and not line.startswith('└'):
                    line = parse_line(line)
                    if line in parts:
                        line += '-extra'
                    parts.add(line)
    return parts

def is_blank_char(c):
    return c in [' ', '\n', '\t']

def is_brace_char(c):
    return c in ['{', '[', '(', ')', ']', '}']

def is_break_char(c):
    return (c in [',', ';']) or is_brace_char(c) or is_blank_char(c)

def match_ph_word(line):
    i = 0
    while is_blank_char(line[i]):
        i += 1
    found = False
    while not is_break_char(line[i]):
        i += 1
        found = True
    if not found:
        return 0
    return i

# TODO: Support more place holders, eg: {#NUMBER}
def compare_line(line, template):
    while True:
        i = template.find(WORD_PH)
        if i < 0:
            return line == template
        else:
            if line[:i] != template[:i]:
                return False
            j = match_ph_word(line[i:])
            if j == 0:
                return False
            template = template[i + len(WORD_PH):]
            line = line[i + j:]

def matched(outputs, matches, fuzz, beeline_cmd=False):
    if len(outputs) == 0 and len(matches) == 0:
        return True

    is_table_parts = beeline_cmd or (len(matches) > 0 and matches[0].startswith('┌'))
    if is_table_parts:
        a = parse_table_parts(outputs, fuzz, beeline_cmd)
        b = parse_table_parts(matches, fuzz, beeline_cmd)
        return a == b
    else:
        if len(outputs) != len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
        return True

class Matcher:
    def __init__(self, executor, executor_ops, fuzz):
        self.executor = executor
        self.executor_ops = executor_ops
        self.fuzz = fuzz
        self.query = None
        self.outputs = None
        self.matches = []
        self.beeline_cmd = False

    def on_line(self, line):
        if line.startswith(SLEEP_PREFIX):
            time.sleep(float(line[len(SLEEP_PREFIX):]))
        elif line.startswith(CMD_PREFIX_TI_MYSQL):
            self.executor_ops.exe('mysql', line[len(CMD_PREFIX_TI_MYSQL):])
        elif line.startswith(CMD_PREFIX_TI_BEELINE):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.beeline_cmd):
                return False
            self.query = line[len(CMD_PREFIX_TI_BEELINE):]
            self.outputs = self.executor_ops.exe('beeline', self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.outputs = filter(lambda x: x.find('|') != -1 or x.find('+') != -1, self.outputs)
            self.outputs = map(lambda x: x.split(' ', 1)[1] if x.startswith('[') else x, self.outputs)
            self.matches = []
            self.beeline_cmd = True
        elif line.startswith(CMD_PREFIX) or line.startswith(CMD_PREFIX_ALTER):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.beeline_cmd):
                return False
            self.query = line[len(CMD_PREFIX):]
            self.outputs = self.executor.exe(self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.matches = []
            self.beeline_cmd = False
        else:
            self.matches.append(line)
        return True

    def on_finish(self):
        if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.beeline_cmd):
            return False
        return True

def parse_exe_match(path, executor, executor_ops, fuzz):
    todos = []
    with open(path) as file:
        matcher = Matcher(executor, executor_ops, fuzz)
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

def run():
    if len(sys.argv) != 6:
        print 'usage: <bin> tiflash-client-cmd test-file-path ti-sh-path ti-file-path fuzz-check'
        sys.exit(1)

    dbc = sys.argv[1]
    test_file_path = sys.argv[2]
    ti_sh_path = sys.argv[3]
    ti_file_path = sys.argv[4]
    fuzz = (sys.argv[5] == 'true')

    matched, matcher, todos = parse_exe_match(test_file_path, Executor(dbc), OpsExecutor(ti_sh_path, ti_file_path), fuzz)

    def display(lines):
        if len(lines) == 0:
            print ' ' * 4 + '<nothing>'
        else:
            for it in lines:
                print ' ' * 4 + it

    if not matched:
        print '  File:', test_file_path
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

def main():
    try:
        run()
    except KeyboardInterrupt:
        print 'KeyboardInterrupted'
        sys.exit(1)

main()
