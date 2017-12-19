#pragma once

#include <DataStreams/BlockIO.h>

namespace Magic
{

class SafeBlockIO
{
public:
    enum { UnknownSize = size_t(-1) };

    SafeBlockIO(DB::BlockIO & input_) : input(input_), closed(false), block_count(0), batch_count(0)
    {
        std::lock_guard<std::mutex> lock(mutex);
        input.in->readPrefix();
    }

    DB::Block sample()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return input.in_sample;
    }

    void read(std::vector<DB::Block> & dest, bool & closed, size_t batch_size = 4)
    {
        std::lock_guard<std::mutex> lock(mutex);

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
        std::lock_guard<std::mutex> lock(mutex);

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
        std::lock_guard<std::mutex> lock(mutex);
        close(exception);
    }

    size_t batches()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (closed)
            return batch_count;
        else
            return UnknownSize;
    }

    size_t blocks()
    {
        std::lock_guard<std::mutex> lock(mutex);
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
    std::mutex mutex;

    DB::BlockIO & input;
    bool closed;
    size_t block_count;
    size_t batch_count;

    size_t interactive_delay_ms;
};

}
