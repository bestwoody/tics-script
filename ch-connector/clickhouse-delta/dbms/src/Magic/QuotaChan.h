#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>

namespace Magic {

using std::condition_variable;
using std::mutex;
using std::queue;
using std::unique_lock;

template <typename T>
class QuotaChan
{
    queue<T> que;
    mutable mutex mtx;

    condition_variable cond_r;
    condition_variable cond_w;

    long quota;
    const size_t capacity;
    size_t passed;
    bool closed;

public:
    inline QuotaChan(long quota_ = -1, size_t capacity_ = 0) : quota(quota_), capacity(capacity_), passed(0), closed(false)
    {
    }

    inline void setQuota(long quota_)
    {
        {
            unique_lock<mutex> lock{mtx};
            quota = quota_;
        }
        cond_w.notify_all();
        cond_r.notify_all();
    }

    inline bool empty() const
    {
        unique_lock<mutex> lock{mtx};
        return que.empty();
    }

    inline void close()
    {
        {
            unique_lock<mutex> lock{mtx};
            closed = true;
        }
        cond_w.notify_all();
        cond_r.notify_all();
    }

    inline size_t size() const
    {
        unique_lock<mutex> lock{mtx};
        return que.size();
    }

    inline void push(const T &v)
    {
        {
            unique_lock<mutex> lock{mtx};
            while (capacity != 0 && capacity <= que.size() && (quota < 0 || passed < size_t(quota)) && !closed)
                cond_w.wait(lock);
            if (closed)
                return;
            que.push(v);
        }
        cond_r.notify_one();
    }

    inline bool pop(T &v)
    {
        unique_lock<mutex> lock{mtx};
        if ((quota >= 0 && passed >= size_t(quota)) || closed)
            return false;

        while (que.empty() && (quota < 0 || passed < size_t(quota)) && !closed)
            cond_r.wait(lock);

        if (que.empty() || closed)
            return false;

        v = que.front();
        que.pop();
        ++passed;

        cond_w.notify_all();

        if (quota >= 0 && passed >= size_t(quota))
            cond_r.notify_all();
        return true;
    }
};

}
