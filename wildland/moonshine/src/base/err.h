#pragma once

#include <string>
#include <ostream>
#include <sstream>

namespace moonshine {

using std::string;
using std::ostream;
using std::stringstream;

struct Err {
    string msg;

    Err() {}
    Err(const string &msg_) : msg(msg_) {}

    bool Empty() const {
        return msg.size() == 0;
    }

    friend ostream & operator <<(ostream &w, const Err &e) {
        return w << "exception: " <<  e.msg;
    }
};

struct ErrNoImpl : public Err {
    inline ErrNoImpl(const string &msg) : Err("no implement: " + msg) {}
};

struct ErrWrongUsage : public Err {
    ErrWrongUsage(const string &msg) : Err(msg) {}
};

}
