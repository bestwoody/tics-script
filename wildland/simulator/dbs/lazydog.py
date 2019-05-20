import base
import db

class Partition:
    def __init__(self, hw, files = []):
        self.hw = hw
        self.write_cache = db.SegCache()
        self.files = []
        for file in files:
            self.files.append(file)
            file.partitions.add(self)

    def write(self, k):
        self.write_cache.write(k)
        self.hw.disk.on_seq_write(1)

    def size_in_files(self):
        real, stored = 0, 0
        for file in self.files:
            if len(file.partitions) > 1:
                real += file.dist.size / len(file.partitions)
            else:
                real += file.dist.size
            stored += file.dist.size
        return real, stored

    def size_in_cache(self):
        return self.write_cache.size()

    def min(self):
        min = self.write_cache.dist.min
        for file in self.files:
            if min > file.dist.min:
                min = file.dist.min
        return min

    def max(self):
        max = self.write_cache.dist.max
        for file in self.files:
            if max < file.dist.max:
                max = file.dist.max
        return max

    def is_desc(self):
        if not self.write_cache.dist.is_desc():
            return False
        for file in self.files:
            if not file.dist.is_desc():
                return False
            if file.dist.intersect(self.write_cache.dist):
                return False
        return True

    def is_asc(self):
        if not self.write_cache.dist.is_asc():
            return False
        for file in self.files:
            if not file.dist.is_asc():
                return False
            if file.dist.intersect(self.write_cache.dist):
                return False
        return True

    # TODO: better splitting, use all dist info (not just dist in write cache)
    def split(self, my_range_begin, my_range_end):
        '''return: n splited_partitions, n-1 split_point'''
        self._persist_write_cache()

        # Keep self so we don't need to clean ref-partitions from existing files
        if my_range_begin == base.Limit.minint and self.is_desc():
            return [Partition(self.hw), self], [self.min()]
        elif my_range_end == base.Limit.maxint and self.is_asc():
            return [self, Partition(self.hw)], [self.max() + 1]
        else:
            return [self, Partition(self.hw, self.files)], [(self.min() + self.max()) / 2]

    def _persist_write_cache(self):
        file = db.File(self.write_cache.dist)
        file.partitions.add(self)
        self.files.append(file)
        self.write_cache = db.SegCache()

class LazyDog(db.DB):
    def __init__(self, hw, partition_split_size, seg_min_size, auto_compact):
        db.DB.__init__(self, hw, auto_compact)
        self._partition_split_size = partition_split_size
        self._seg_min_size = seg_min_size

        # [begin, end) for each partition
        self._boundaries = [base.Limit.minint, base.Limit.maxint]
        self.partitions = [Partition(self.hw)]

    def write(self, k):
        db.DB.write(self, k)
        i = base.upper_bound(self._boundaries, k) - 1
        partition = self.partitions[i]
        partition.write(k)
        size_in_file, x = partition.size_in_files()
        #print 'W', k, 'in-cache:', partition.size_in_cache(), 'in-file:', size_in_file, x
        if partition.size_in_cache() + size_in_file >= self._partition_split_size:
            self._split_partition(i, k)

    def scan_all(self, indent = 0, show_id = True, show_detail = True):
        id = lambda obj, name: show_id and name + base.obj_id(obj) or name
        msg = []

        for i in range(0, len(self.partitions)):
            partition, begin, end = self._partition_status(i)
            real_size, stored_size = partition.size_in_files()
            cached_size = partition.size_in_cache()

            msg += [' ' * indent, 'partition', id(partition, ''), ' #', i]
            msg += [' range[', base.i2s(begin), ', ', base.i2s(end), ')\n']
            if show_detail:
                if len(partition.files) > 0 or cached_size > 0:
                    msg += [' ' * (indent + 2), 'stored-min-max: ', base.i2s(partition.min()),
                        ', ', base.i2s(partition.max()), '\n']
                if partition.write_cache.size() > 0:
                    msg += [' ' * (indent + 2)] + partition.write_cache.str(show_id, show_detail) + ['\n']
            if len(partition.files) > 0:
                if show_detail:
                    msg += [' ' * (indent + 2), 'disk-size: real=', real_size, ', stored=', stored_size, '\n']
                    msg += [' ' * (indent + 2), 'files: ', len(partition.files), '\n']
                for file in partition.files:
                    # TODO: cache file
                    for seg in file.dist.segs:
                        self.hw.disk.on_seq_read(seg.size)
                    if show_detail:
                        msg += file.str(indent + 4, show_id)
            streams = (cached_size > 0 and 1 or 0) + len(partition.files)
            size = cached_size + stored_size
            if not partition.is_asc() and not partition.is_desc():
                self.hw.processor.sorter.on_sort(size)
                msg += [' ' * (indent + 2), 'sort(disk_read(', stored_size, ') + mem:', cached_size, ')\n']
            elif streams > 1:
                self.hw.processor.sorter.on_sorted_merge(size)
                msg += [' ' * (indent + 2), 'sorted_merge(disk_read(', stored_size, ', ',
                    len(partition.files), ' files) + mem:', cached_size, ')\n']
            elif stored_size > 0:
                msg += [' ' * (indent + 2), 'disk_read(', stored_size, ')\n']
            elif cached_size > 0:
                msg += [' ' * (indent + 2), 'get(mem:', cached_size, ')\n']
            else:
                pass
        return msg

    def manually_compact(self):
        pass

    def _partition_status(self, i):
        return self.partitions[i], self._boundaries[i], self._boundaries[i + 1]

    def _split_partition(self, i, k):
        partitions, split_points = self.partitions[i].split(
            self._boundaries[i], self._boundaries[i + 1])
        #print self._boundaries, '<=', k, split_points
        if len(partitions) == 1:
            return
        self.partitions.pop(i)
        partitions.reverse()
        for p in partitions:
            self.partitions.insert(i, p)
        split_points.reverse()
        for b in split_points:
            self._boundaries.insert(i + 1, b)
