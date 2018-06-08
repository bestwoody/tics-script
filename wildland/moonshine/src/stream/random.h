#pragma once

#include "data/stream.h"

namespace moonshine {

class GenRandBlock {
public:
    static void PrepareSeed() {
        srand((int)time(0));
    }

    GenRandBlock(const Schema &schema, size_t block_rows_, size_t prepare_blocks) : block_rows(block_rows_) {
        PrepareSeed();
        prepared.resize(prepare_blocks);
        for (Block &block: prepared) {
            block.FillRand(schema, block_rows);
        }
    }

    Block Get() const {
        size_t number = rand();
        return prepared[number % prepared.size()];
    }

    Block Get(size_t pos) const {
        return prepared[pos];
    }

    size_t PreparedBlocks() const {
        return prepared.size();
    }

    const size_t block_rows;

private:
    vector<Block> prepared;
};

class StreamRandom : public IBlocksInput {
public:
    StreamRandom(const GenRandBlock &gen_, size_t total_rows_) :
        gen(gen_), total_rows(total_rows_), handled_blocks(0), handled_rows(0) {}

    const Block Read() override {
        Block block = (gen.PreparedBlocks() >= (total_rows + gen.block_rows - 1) / gen.block_rows) ? gen.Get(handled_blocks++) : gen.Get();
        size_t rows = min(gen.block_rows, total_rows - handled_rows);
        block.Resize(rows);
        handled_rows += rows;
        return block;
    }

    bool Done() override {
        return handled_rows >= total_rows;
    }

private:
    const GenRandBlock &gen;
    size_t total_rows;
    size_t handled_blocks;
    size_t handled_rows;
};

}
