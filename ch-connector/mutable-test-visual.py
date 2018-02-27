import sys
import os

def gen(output, title, rows, gn, order):
    # TODO
    path = os.path.join(output, title + '_g' + str(gn) + '_o' + str(order) + '.test')
    print path
    for row in rows:
        print ' ' * 4 + row

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
                line = filter(lambda x: x != '-' and x != '+', line)
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
                    rows.append(line)

def main():
    if len(sys.argv) != 2:
        print 'usage: <bin> visual-test-file-path'
        sys.exit(1)

    path = sys.argv[1]
    output = os.path.dirname(path)
    parse_and_gen(path, output)

main()
