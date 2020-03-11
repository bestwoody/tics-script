# -*- coding:utf-8 -*-

import os
import sys
import time

CMD_PREFIX_TI_MYSQL = 'ti_mysql> '
CMD_PREFIX_TI_MYSQL_COP = 'ti_mysql_cop> '
CMD_PREFIX_TI_MYSQL_IGNORE_OUTPUT = 'ti_mysql_ignore> '
CMD_PREFIX_TI_BEELINE = 'ti_beeline> '
CMD_PREFIX_TI_CH = 'ti_ch> '
CMD_SHELL_FUNC = 'ti_func> '
RETURN_PREFIX = '#RETURN'
SLEEP_PREFIX = 'SLEEP '
TODO_PREFIX = '#TODO'
COMMENT_PREFIX = '#'
UNFINISHED_1_PREFIX = '\t'
UNFINISHED_2_PREFIX = '   '
WORD_PH = '{#WORD}'

class QueryType:
    ti_beeline_tag = "ti_beeline"
    ti_ch_tag = "ti_ch"
    ti_mysql_tag = "ti_mysql"
    ti_mysql_cop_tag = "ti_mysql_cop"
    ti_mysql_ignore_tag = "ti_mysql_ignore"
    func_tag = "func"
    def __init__(self, query_type):
        self.query_type = query_type
    @classmethod
    def ti_beeline(cls):
        return cls(cls.ti_beeline_tag)
    @classmethod
    def ti_ch(cls):
        return cls(cls.ti_ch_tag)
    @classmethod
    def ti_mysql(cls):
        return cls(cls.ti_mysql_tag)
    @classmethod
    def ti_mysql_cop(cls):
        return cls(cls.ti_mysql_cop_tag)
    @classmethod
    def ti_mysql_ignore(cls):
        return cls(cls.ti_mysql_ignore_tag)
    @classmethod
    def func(cls):
        return cls(cls.func_tag)
    def is_ti_beeline(self):
        return self.query_type == self.ti_beeline_tag
    def is_ti_ch(self):
        return self.query_type == self.ti_ch_tag
    def is_ti_mysql(self):
        return self.query_type == self.ti_mysql_tag
    def is_ti_mysql_cop(self):
        return self.query_type == self.ti_mysql_cop_tag
    def is_ti_mysql_ignore(self):
        return self.query_type == self.ti_mysql_ignore_tag
    def is_func(self):
        return self.query_type == self.func_tag
    def __str__(self):
        return self.query_type

class ShellFuncExecutor:
    def __init__(self, test_ti_file):
        self.test_ti_file = test_ti_file
    def exe(self, cmd):
        return os.popen((cmd + ' ' + self.test_ti_file + ' 2>&1').strip()).readlines()

# A mysql or mysql_cop query statement could specify an optional session isolation engine (tikv, tiflash, or both - concatenated by comma and no space) at head.
# If specified, this method prepends an individual session isolation engine setting statement to overcome the limitation that we use separate mysql sessions for different statements.
def parse_mysql_query_si(query, explain):
    splits = query.split(" ")
    query_prefix = ""
    if explain:
        query_prefix = "explain "
    if splits[0] == "tikv" or splits[0] == "tiflash" or splits[0] == "tikv,tiflash" or splits[0] == "tiflash,tikv":
        si = splits[0]
        actual_query = " ".join(splits[1:])
        return ["set @@session.tidb_isolation_read_engines='" + si + "'", query_prefix + actual_query]
    return [query_prefix + query]

class OpsExecutor:
    def __init__(self, ti_sh, test_ti_file):
        self.ti_sh = ti_sh
        self.test_ti_file = test_ti_file
    def exe(self, cmd_type, cmd):
        ops_cmd = ""
        cmd_arr = cmd.split("--")
        if len(cmd_arr) <= 0:
            return ""
        query = cmd_arr[0]
        padding_args = []
        if cmd_type.is_ti_beeline():
            ops_cmd = "beeline"
        elif cmd_type.is_ti_mysql() or cmd_type.is_ti_mysql_ignore():
            ops_cmd = "mysql"
            query_arr = parse_mysql_query_si(query, False)
            query = ";".join(query_arr)
            padding_args = ['"test"', 'false', 'true', 'false']
        elif cmd_type.is_ti_mysql_cop():
            ops_cmd = "mysql"
            query_arr = parse_mysql_query_si(query, True)
            query = ";".join(query_arr)
            padding_args = ['"test"', 'false', 'true', 'true']
        elif cmd_type.is_ti_ch():
            ops_cmd = "ch"
            padding_args = ['default', 'pretty', 'false']
        else:
            raise Exception("Unknown command type", str(cmd_type))
        args = map(lambda x: "--" + x.strip(), cmd_arr[1:])
        cmd = ""
        cmd += self.ti_sh
        cmd += ' "' + self.test_ti_file + '"'
        cmd += ' ' + ops_cmd
        cmd += ' "' + query + '"'
        cmd += ' ' + " ".join(padding_args)
        cmd += ' ' + " ".join(args)
        cmd += ' 2>&1'
        return os.popen(cmd.strip()).readlines()

def parse_line(line):
    words = [w.strip() for w in line.split("│") if w.strip() != ""]
    return "@".join(words)

def parse_beeline_line(line):
    words = [w.strip() for w in line.split("|") if w.strip() != ""]
    return "@".join(words)

def parse_mysql_line(line):
    words = [w.strip() for w in line.split("|") if w.strip() != ""]
    return "@".join(words)

def parse_beeline_table_parts(lines, fuzz):
    parts = set()
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
    return parts

def parse_ch_table_parts(lines, fuzz):
    parts = set()
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

def parse_mysql_table_parts(lines):
    results = set()
    for line in lines:
        if line.startswith('+'):
            continue
        line = parse_mysql_line(line)
        while line in results:
            line += '-extra'
        results.add(line)
    return results

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

def ti_beeline_matched(outputs, matches, fuzz):
    is_table_parts = len(matches) > 0 and matches[0].startswith('+')
    if is_table_parts:
        a = parse_beeline_table_parts(outputs, fuzz)
        b = parse_beeline_table_parts(matches, fuzz)
        return a == b
    else:
        if len(outputs) != len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
        return True

def ch_matched(outputs, matches, fuzz):
    is_table_parts = len(matches) > 0 and matches[0].startswith('┌')
    if is_table_parts:
        a = parse_ch_table_parts(outputs, fuzz)
        b = parse_ch_table_parts(matches, fuzz)
        return a == b
    else:
        if len(outputs) != len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
        return True

def mysql_matched(outputs, matches):
    is_table_parts = len(matches) > 0 and matches[0].startswith('+')
    if is_table_parts:
        a = parse_mysql_table_parts(outputs)
        b = parse_mysql_table_parts(matches)
        return a == b
    else:
        if len(outputs) != len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
    return True

def mysql_cop_matched(outputs, matches):
    match_idx = 0
    for output in outputs:
        if match_idx == len(matches):
            break
        if matches[match_idx] in output:
            match_idx += 1
    return match_idx == len(matches)

def matched(outputs, matches, fuzz, query_type):
    if len(outputs) == 0 and len(matches) == 0:
        return True

    if query_type.is_ti_beeline():
        return ti_beeline_matched(outputs, matches, fuzz)
    elif query_type.is_ti_ch():
        return ch_matched(outputs, matches, fuzz)
    elif query_type.is_ti_mysql():
        return mysql_matched(outputs, matches)
    elif query_type.is_ti_mysql_cop():
        return mysql_cop_matched(outputs, matches)
    elif query_type.is_ti_mysql_ignore():
        raise Exception("The result of sql begin with ti_mysql_ignore> should be ignored")
    else:
        raise Exception("Unknown query type", str(query_type))

class Matcher:
    def __init__(self, executor_func, executor_ops, fuzz):
        self.executor_func = executor_func
        self.executor_ops = executor_ops
        self.fuzz = fuzz
        self.query = None
        self.query_type = None
        self.query_line_number = 0
        self.outputs = None
        self.extra_outputs = None
        self.matches = []

    def on_line(self, line, line_number):
        if line.startswith(SLEEP_PREFIX):
            time.sleep(float(line[len(SLEEP_PREFIX):]))
        elif line.startswith(CMD_SHELL_FUNC):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.executor_func.exe(line[len(CMD_SHELL_FUNC):])
            self.query_type = QueryType.func()
            self.outputs = None
            self.extra_outputs = None
        elif line.startswith(CMD_PREFIX_TI_MYSQL):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.query_line_number = line_number
            self.query = line[len(CMD_PREFIX_TI_MYSQL):]
            self.query_type = QueryType.ti_mysql()
            self.outputs = self.executor_ops.exe(self.query_type, self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.extra_outputs = None
            self.matches = []
        elif line.startswith(CMD_PREFIX_TI_MYSQL_COP):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.query_line_number = line_number
            self.query = line[len(CMD_PREFIX_TI_MYSQL_COP):]
            self.query_type = QueryType.ti_mysql_cop()
            self.outputs = self.executor_ops.exe(self.query_type, self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.extra_outputs = None
            self.matches = []
        elif line.startswith(CMD_PREFIX_TI_MYSQL_IGNORE_OUTPUT):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.query_line_number = line_number
            self.query = line[len(CMD_PREFIX_TI_MYSQL_IGNORE_OUTPUT):]
            self.query_type = QueryType.ti_mysql_ignore()
            self.executor_ops.exe(self.query_type, self.query)
            self.outputs = []
            self.extra_outputs = None
            self.matches = []
        elif line.startswith(CMD_PREFIX_TI_BEELINE):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.query_line_number = line_number
            self.query = line[len(CMD_PREFIX_TI_BEELINE):]
            self.query_type = QueryType.ti_beeline()
            self.outputs = self.executor_ops.exe(self.query_type, self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            def is_beeline_table_parts(x):
                return x.find('|') != -1 or x.find('+') != -1
            self.extra_outputs = filter(lambda x: not is_beeline_table_parts(x), self.outputs)
            self.outputs = filter(is_beeline_table_parts, self.outputs)
            self.outputs = map(lambda x: x.split(' ', 1)[1] if x.startswith('[') else x, self.outputs)
            self.matches = []
        elif line.startswith(CMD_PREFIX_TI_CH):
            if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
                return False
            self.query_line_number = line_number
            self.query = line[len(CMD_PREFIX_TI_CH):]
            self.query_type = QueryType.ti_ch()
            self.outputs = self.executor_ops.exe(self.query_type, self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.outputs = map(lambda x: x.split(' ', 1)[1] if x.startswith('[') else x, self.outputs)
            self.extra_outputs = None
            self.matches = []
        else:
            self.matches.append(line)
        return True

    def on_finish(self):
        if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz, self.query_type):
            return False
        return True

def parse_exe_match(path, executor_func, executor_ops, fuzz):
    todos = []
    line_number = 0
    line_number_cached = 0
    with open(path) as file:
        matcher = Matcher(executor_func, executor_ops, fuzz)
        cached = None
        for origin in file:
            line_number += 1
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
            if cached != None and not matcher.on_line(cached, line_number_cached):
                return False, matcher, todos
            cached = line
            line_number_cached = line_number
        if (cached != None and not matcher.on_line(cached, line_number)) or not matcher.on_finish():
            return False, matcher, todos
        return True, matcher, todos

def run():
    if len(sys.argv) != 5:
        print 'usage: <bin> tiflash_client_cmd test_file_path ti_sh ti_file fuzz_check'
        sys.exit(1)

    test_file_path = sys.argv[1]
    ti_sh = sys.argv[2]
    ti_file = sys.argv[3]
    fuzz = (sys.argv[4] == 'true')

    matched, matcher, todos = parse_exe_match(test_file_path, ShellFuncExecutor(ti_file),
                                              OpsExecutor(ti_sh, ti_file), fuzz)

    def display(lines):
        if len(lines) == 0:
            print ' ' * 4 + '<nothing>'
        else:
            for it in lines:
                print ' ' * 4 + it

    if not matched:
        print '  File:', test_file_path
        print '  Error line:', matcher.query_line_number
        print '  Error:', matcher.query
        print '  Result:'
        display(matcher.outputs)
        print '  Expected:'
        display(matcher.matches)
        if matcher.extra_outputs is not None:
            print ' Extra Result:'
            display(matcher.extra_outputs)
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
