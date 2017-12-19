#pragma once

#include <DataStreams/BlockIO.h>
#include <DataStreams/AsynchronousBlockInputStream.h>

namespace Magic
{

class AsyncBlockIO
{
public:
    enum { UnknownSize = size_t(-1) };

    AsyncBlockIO(DB::BlockIO & origin_) : origin(origin_), input(origin.in), closed(false),
        block_count(0), batch_count(0), interactive_delay_ms(300)
    {
        std::lock_guard<std::mutex> lock(mutex);
        input.readPrefix();
    }

    DB::Block sample()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return origin.in_sample;
    }

    void read(std::vector<DB::Block> & dest, bool & closed, size_t batch_size = 4)
    {
        std::lock_guard<std::mutex> lock(mutex);

        closed = this->closed;
        if (closed)
            return;

        while (dest.size() != batch_size)
        {
            DB::Block block;
            while (true)
            {
                if (input.poll(interactive_delay_ms))
                {
                    block = input.read();
                    break;
                }
            }

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

        while (true)
        {
            if (input.poll(interactive_delay_ms))
            {
                block = input.read();
                break;
            }
        }

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
        input.cancel();
        input.readSuffix();
        if (exception)
            origin.onException();
        origin.onFinish();
        closed = true;
    }

private:
    std::mutex mutex;

    DB::BlockIO & origin;
    DB::AsynchronousBlockInputStream input;
    bool closed;
    size_t block_count;
    size_t batch_count;

    size_t interactive_delay_ms;
};

}
