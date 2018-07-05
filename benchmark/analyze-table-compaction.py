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
    level_sizes = []
    level_counts = []
    total_size = 0
    max_end = 0
    io_size = 0

    for name, begin, end, level, size in parts:
        total_size += size
        io_size += size * (level + 1)
        while len(level_counts) <= level:
            level_counts.append(0)
            level_sizes.append(0)
        level_counts[level] += 1
        level_sizes[level] += size
        if end > max_end:
            max_end = end

    level_0_size = 0
    if len(level_counts) > 0 and level_counts[0] != 0:
        level_0_size = float(level_sizes[0]) / level_counts[0]

    for i in range(0, len(level_counts)):
        avg_size = '?'
        if level_counts[i] != 0:
            avg_size = level_sizes[i] / float(level_counts[i])
        level_sizes[i] = 'L' + str(i) + '=' + str(avg_size)
        level_counts[i] = 'L' + str(i) + '=' + str(level_counts[i])

    print "Partitions count:", len(partitions)
    print "Parts count:", len(parts)
    if len(parts) > 0:
        print "Parts avg size(KB):", float(total_size) / len(parts)
    if len(partitions) > 0:
        print "Parts count per partition:", float(len(parts)) / len(partitions)
    print "Parts count of each level:", level_counts
    print "Parts size(KB) of each level:", level_sizes
    print "Approximate write batch size(KB):", len(partitions) * level_0_size
    if len(partitions) > 0:
        print "Approximate finished write batchs count:", float(max_end) / len(partitions)
        print "Approximate write amplification:", float(io_size) / total_size

analyze(*parse())
