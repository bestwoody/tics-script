#pragma once

#include <string>
#include <vector>
#include <algorithm>

#include "data/block.h"
#include "data/sorting.h"
#include "storage/partry/delta.h"

namespace moonshine {

using std::string;
using std::vector;
using std::sort;

class Layer {
public:
    // TODO: partitioning function
    Layer(
        string store_path_,
        size_t level_ = 0,
        size_t min_io_size_ = 32 * 1024,
        bool merge_on_write_ = true,
        bool merge_on_read_ = true) :
        store_path(store_path_),
        level(level_),
        min_io_size(min_io_size_),
        merge_on_write(merge_on_write_),
        merge_on_read(merge_on_read_) {}

    void Write(const SortedSchema &schema, Block &block) {
        block.GenEncodedPKColumn(schema);
        block.SortRowsByPK();

        // If not merge_on_write:
        //   Gather infos, keep them in deltas
        //   Write to a new delta file
        // Else:
        //   If is big enough:
        //     Split and write to fragment dirs
        //     Gather infos, keep them in fragments
        //   Else if all deltas summary size is big enough:
        //     Read other deltas and merge, and split, then write to fragment dirs
        //     Gather infos, keep them in fragments
        //   Else:
        //     Gather infos, keep them in deltas
        //     Write to a new delta file
    }

    void Load() {
        // Find in store path, for each:
        //   Delta file: load infos to deltas
        //   Fragment dir: load infos to fragments
    }

    void TryMerge(bool finalize) {
        // If finalize or all deltas summary size is big enough:
        //   Read enough deltas and merge, and split, then write to fragment dirs
        //   Gather infos, keep them in fragments
    }

    template <typename BlockSink>
    void Scan(BlockSink sink) {
        // Load all deltas, merge and split
        // Scan all fragments, for each:
        //   Load fragment
        //   Find data belong to this fragment in merged delta , merge to fragment
        //   If merge_on_read and merged delta is big enough:
        //     Split and write to fragment dirs
        //     Gather infos, keep them in fragments
    }

private:
    // TODO: use tree instead of vector, for fast finding by PK range
    vector<Delta> deltas;
    vector<Layer> fragments;

    string store_path;
    size_t level;
    size_t min_io_size;
    bool merge_on_write;
    bool merge_on_read;
};

}
