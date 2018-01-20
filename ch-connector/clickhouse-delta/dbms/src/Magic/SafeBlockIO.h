#pragma once

#include <DataStreams/BlockIO.h>

namespace Magic
{

class SafeBlockIO
{
public:
    enum
    {
        UnknownSize = size_t(-1)
    };

    SafeBlockIO(DB::BlockIO & input_) : input(input_), closed(false), block_count(0), batch_count(0)
    {
        input.in->readPrefix();
    }

    DB::Block sample()
    {
        return input.in_sample;
    }

    void read(std::vector<DB::Block> & dest, bool & closed, size_t batch_size = 4)
    {
        closed = this->closed;
        if (closed)
            return;

        while (dest.size() != batch_size)
        {
            DB::Block block = input.in->read();
            if (!block)
            {
                closed = true;
                close();
                break;
            }
            else
            {
                block_count += 1;
                dest.push_back(block);
            }
        }

        if (dest.size())
            batch_count += 1;
    }

    DB::Block read()
    {
        DB::Block block;
        if (closed)
            return block;
        block = input.in->read();

        if (!block)
        {
            close();
        }
        else
        {
            block_count += 1;
            batch_count += 1;
        }

        return block;
    }

    void cancal(bool exception = false)
    {
        close(exception);
    }

    size_t batches()
    {
        if (closed)
            return batch_count;
        else
            return UnknownSize;
    }

    size_t blocks()
    {
        if (closed)
            return block_count;
        else
            return UnknownSize;
    }

private:
    void close(bool exception = false)
    {
        if (closed)
            return;
        input.in->readSuffix();
        if (exception)
            input.onException();
        input.onFinish();
        closed = true;
    }

private:
    DB::BlockIO & input;
    bool closed;
    size_t block_count;
    size_t batch_count;
};

}
