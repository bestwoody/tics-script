import sys
import os

def gen(output, title, rows, num):
    # TODO
    path = os.path.join(output, title + '_' + str(num) + '.test')
    print path
    for row in rows:
        print ' ' * 4 + row

def parse_and_gen(path, output):
    title = ''
    rows = []
    num = 0
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
                    num = 0
            else:
                if len(line) == 0:
                    if len(rows) != 0:
                        gen(output, title, rows, num)
                        num += 1
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
