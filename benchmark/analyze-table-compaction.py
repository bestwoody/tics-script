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

        # Is MergeTree engine partitions
        if len(part_info) == 5:
            part_info = [part_info[0] + '_' + part_info[1]] + part_info[2:]

        # Is MutableMergeTree engine partitions
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
    max_end = 0
    io_size = 0

    for name, begin, end, level, size in parts:
        total_size += size
        io_size += size * (level + 1)
        while len(levels) <= level:
            levels.append(0)
        levels[level] += 1
        if end > max_end:
            max_end = end

        if level != 0:
            continue
        level_0_size += size
        level_0_count += 1

    for i in range(0, len(levels)):
        levels[i] = 'L' + str(i) + '=' + str(levels[i])

    print "Partitions count:", len(partitions)
    if len(partitions) > 0:
        print "Parts count per partition:", float(len(parts)) / len(partitions)
        print "Parts count of each level:", levels
    if len(parts) > 0:
        print "Parts avg size(KB):", float(total_size) / len(parts)
    if level_0_count != 0:
        print "Parts of level-0 avg size(KB):", float(level_0_size) / level_0_count
        print "Approximate write batch size(KB):", len(partitions) * float(level_0_size) / level_0_count
    if len(partitions) > 0:
        print "Approximate finished write batchs count:", float(max_end) / len(partitions)
        print "Approximate write amplification:", float(io_size) / total_size

analyze(*parse())
