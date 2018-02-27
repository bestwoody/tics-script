# -*- coding:utf-8 -*-

import sys
import os

# TODO
class TestData:
    def __init__(self, rows):
        self.rows = rows

    def insert_values(self):
        return [[(10, 10), (11, 11)], [(20, 20)]]

    def select_result_parts(self):
        return [[(10, 10), (11, 11)], [(20, 20)]]

    def selraw_result_parts(self):
        return [[(10, 10, 1000000), (11, 11, 1000001)], [(20, 20, 2000000)]]

def gen(output, title, rows, gn, order):
    path = os.path.join(output, 'dedup_' + title + '_g' + str(gn) + '_o' + str(order) + '.test')
    data = TestData(rows)
    with open(path, "w") as file:
        file.write('# Visualization of table parts and keys:\n')
        for row in rows:
            file.write('# ' + row + '\n')
        file.write('\n')
        file.write('>> drop table if exists test\n')
        file.write('>> create table test (\n')
        file.write('\tdt Date,\n')
        file.write('\tk Int32,\n')
        file.write('\tv Int32\n')
        file.write('\t) engine = MutableMergeTree(dt, (k), 8192)\n')
        file.write('\n')
        for values in data.insert_values():
            file.write('>> insert into test values ')
            for i in range(0, len(values)):
                k, v = values[i]
                file.write('(0, ' + str(k) + ', ' + str(v) + ')')
                if i != len(values) - 1:
                    file.write(', ')
                else:
                    file.write('\n')
        file.write('\n')
        file.write('>> select * from test\n')
        for parts in data.select_result_parts():
            file.write('┌─────────dt─┬──k─┬──v─┐\n')
            for k, v in parts:
                file.write('│ 0000-00-00 │ ' + str(k) + ' │ ' + str(v) + ' │\n')
            file.write('└────────────┴────┴────┘\n')
        file.write('\n')
        file.write('>> selraw * from test\n')
        for parts in data.selraw_result_parts():
            file.write('┌─────────dt─┬──k─┬──v─┬─_INTERNAL_VERSION─┬─_INTERNAL_DELMARK─┐\n')
            for k, v, ver in parts:
                file.write('│ 0000-00-00 │ ' + str(k) + ' │ ' + str(v) + ' │           ' + str(ver) + ' │                 0 │\n')
            file.write('└────────────┴────┴────┴───────────────────┴───────────────────┘\n')
        file.write('\n')
        file.write('>> drop table if exists test\n')

    print path

class IdGen:
    def __init__(self):
        self.id = 0
    def get(self):
        self.id += 1
        return self.id

def gen_diff_orders(idg, output, title, rows, gn):
    if len(rows) == 1 or len(rows) == 2 and rows[0] == rows[1]:
        gen(output, title, rows, gn, idg.get())
        return
    def perm(array, begin, end):
        if begin >= end:
            gen(output, title, map(lambda x: rows[x], array), gn, idg.get())
        else:
            i = begin
            for n in range(begin, end):
                array[n], array[i] = array[i], array[n]
                perm(array, begin + 1, end)
                array[n], array[i] = array[i], array[n]
    perm(range(0, len(rows)), 0, len(rows))

def parse_and_gen(path, output):
    title = ''
    rows = []
    gn = 0
    with open(path) as file:
        for origin in file:
            line = origin.strip()
            if line.startswith('##'):
                continue
            if line.startswith('#'):
                line = line[1:].strip().lower()
                line = filter(lambda x: x != '-' and x != '+' and x != '.' and x != ',', line)
                line = map(lambda x: (x >= 'a' and x <= 'z' or x >= '0' and x <= '9') and x or '_', line)
                line = ''.join(line).strip('_')
                if len(line) != 0:
                    title = line
                    gn = 0
            else:
                if len(line) == 0:
                    if len(rows) != 0:
                        gen_diff_orders(IdGen(), output, title, rows, gn)
                        gn += 1
                    rows = []
                else:
                    rows.append(origin.strip('\n'))

def main():
    if len(sys.argv) != 2:
        print 'usage: <bin> visual-test-file-path'
        sys.exit(1)

    path = sys.argv[1]
    output = os.path.dirname(path)
    parse_and_gen(path, output)

main()
