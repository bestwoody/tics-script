import base
import dbs
import hw
import partition

class LazyDog(partition.DB):
    class Partition(partition.Partition):
        def __init__(self, env, seg_min, seg_max, files = []):
            env.log.debug('new partition:', base.obj_id(self))
            partition.Partition.__init__(self, env, seg_min, seg_max, files)

        def clone(self, share_data):
            assert self._write_cache.size() == 0, 'write_cache should be persisted before partition split'
            files = share_data and self._files or []
            return LazyDog.Partition(self.env, self.seg_min, self.seg_max, files)

        def find_split_point(self, my_range_begin, my_range_end, prev_partition, next_partition):
            res = 0
            min, max = self.min(), self.max()
            prev_empty = prev_partition and prev_partition.empty()
            next_empty = next_partition and next_partition.empty()

            assert self._write_cache.info().size > 0
            sorted = self._write_cache.info().sorted
            asc = (sorted == base.Sorted.asc or sorted == base.Sorted.both)
            desc = (sorted == base.Sorted.desc or sorted == base.Sorted.both)

            if my_range_begin == base.Limit.minint and desc and not prev_empty:
                res = min
                self.env.log.debug('split at min:', min)
            elif my_range_end == base.Limit.maxint and asc and not next_empty:
                self.env.log.debug('split at max + 1:', max + 1)
                res = max + 1
            else:
                min = (my_range_begin != base.Limit.minint) and my_range_begin or min
                max = (my_range_end != base.Limit.maxint) and my_range_end or max
                self.env.log.debug('split at (min + max) / 2:', min, max)
                res = (min + max) / 2
            return res

    def __init__(self, env, seg_min, seg_max, partition_split_size, wal_group_size):
        first_partition = LazyDog.Partition(env, seg_min, seg_max)
        partition.DB.__init__(self, env, partition_split_size, wal_group_size, first_partition)
