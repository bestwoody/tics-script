import sys

def parse():
    partitions = {}
    total_parts = []
    detached = 0
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        if len(line) == 0:
            continue
        line = line[:-1]
        if len(line) == 0:
            continue

        fields = line.split()
        size = int(fields[0])
        if (fields[1] == 'detached'):
            detached = size
            continue
        if (fields[1] == 'format_version.txt'):
            continue

        part_info = fields[1].split('_')
        if len(part_info) != 4:
            continue

        part_name, part_begin, part_end, part_level = part_info
        part_begin = int(part_begin)
        part_end = int(part_end)
        part_level = int(part_level)

        parts = []
        if partitions.has_key(part_name):
            parts = partitions[part_name]

        inactived = False
        for name, begin, end, level in parts:
            if part_begin >= begin and end >= part_end:
                inactived = True
                break
        if inactived:
            continue

        total_parts.append((part_name, part_begin, part_end, part_level, size))
        parts.append((part_name, part_begin, part_end, part_level))
        partitions[part_name] = parts

    return partitions, total_parts

def analyze(partitions, parts):
    level_0_size = 0
    level_0_count = 0
    total_size = 0
    levels = []
    for name, begin, end, level, size in parts:
        total_size += size
        while len(levels) <= level:
            levels.append(0)
        levels[level] += 1
        if level != 0:
            continue
        level_0_size += size
        level_0_count += 1

    for i in range(0, len(levels)):
        levels[i] = (i, levels[i])

    print "Partitions count:", len(partitions)
    print "Avg parts per partition:", float(len(parts)) / len(partitions)
    print "Parts avg size:", float(total_size) / len(parts), 'KB'
    if level_0_count != 0:
        print "Level-0 parts avg size:", float(level_0_size) / level_0_count, 'KB'
    print "Each level parts count:", levels

analyze(*parse())
