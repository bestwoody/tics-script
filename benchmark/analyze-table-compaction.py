import sys

def parse():
    partitions = {}
    total_parts = []
    detached = 0
    is_mmt = False

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
        is_mmt = True

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

    return partitions, total_parts, is_mmt

def analyze(partitions, parts, is_mmt):
    level_total_sizes = []
    level_counts = []
    total_size = 0
    max_end = 0
    io_size = 0

    for name, begin, end, level, size in parts:
        total_size += size
        io_size += size * (level + 1)
        while len(level_counts) <= level:
            level_counts.append(0)
            level_total_sizes.append(0)
        level_counts[level] += 1
        level_total_sizes[level] += size
        if end > max_end:
            max_end = end
    total_size = float(total_size) / 1024
    io_size = float(io_size) / 1024

    level_0_size = 0
    if len(level_counts) > 0 and level_counts[0] != 0:
        level_0_size = float(level_total_sizes[0]) / level_counts[0]
    level_0_size /= 1024

    level_avg_sizes = []
    for i in range(0, len(level_counts)):
        level_avg_sizes.append(None)
        if level_counts[i] == 0:
            level_total_sizes[i] = None
            level_avg_sizes[i] = None
            level_counts[i] = None
            continue
        avg_size = level_total_sizes[i] / float(level_counts[i])
        level_total_sizes[i] = 'L' + str(i) + '=' + format(float(level_total_sizes[i]) / 1024, '.2f')
        level_avg_sizes[i] = 'L' + str(i) + '=' + format(float(avg_size) / 1024, '.2f')
        level_counts[i] = 'L' + str(i) + '=' + str(level_counts[i])
    level_total_sizes = filter(lambda x: x != None, level_total_sizes)
    level_avg_sizes = filter(lambda x: x != None, level_avg_sizes)
    level_counts = filter(lambda x: x != None, level_counts)

    print "Partitions count:", len(partitions)
    print "Parts total count:", len(parts)
    if len(parts) > 0:
        print "Parts avg size(MB): %.3f" % (float(total_size) / len(parts))
    if len(partitions) > 0:
        print "Parts count per partition: %.1f" % (float(len(parts)) / len(partitions))
    print "Parts count of each level:", level_counts
    print "Parts level-total size(MB) of each level:", level_total_sizes
    print "Parts avg size(MB) of each level:", level_avg_sizes
    if is_mmt:
        write_batch = "?"
        if level_0_size != 0:
            write_batch = format(level_0_size, '.2f') + " - "
            write_batch += format(len(partitions) * level_0_size, '.2f')
        print "Approximate write batch size(MB):", write_batch
    if len(partitions) > 0:
        print "Approximate finished write batchs count:", float(max_end) / len(partitions), "-", float(max_end)
        print "Approximate write amplification: %.1f" % (float(io_size) / total_size)

analyze(*parse())
