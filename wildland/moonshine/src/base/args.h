#pragma once

// The only reason we use 'g++-7/8' and 'c++17' for now
#include <variant>

#include <string>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "base/err.h"

namespace moonshine {

using std::variant;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::unordered_set;
using std::get;

using Arg = variant<string, bool, int64_t>;

struct Args : public unordered_map<string, Arg> {
    template <typename T>
    T Get(const string &name) const {
        auto it = find(name);
        if (it == end())
            throw ErrWrongUsage("name '" + name + "' not in args");
        return get<T>(at(name));
    }

    template <typename T>
    T Get(const string &name, const T &default_value) const {
        auto it = find(name);
        if (it == end())
            return default_value;
        return get<T>(it->second);
    }
};

struct ExpectedArg {
    bool optional;
    string name;
    string type;
    string help;
};

class ExpectedArgs {
public:
    void AddExpect(bool optional, const string &name, const string &type, string help) {
        ExpectedArg arg{optional, name, type, help};
        if (!optional)
            obligatory_args[name] = arg;
        else
            optional_args[name] = arg;
    }

    string MatchedGetHelp(const Args &args) {
        stringstream not_supported_help;
        unordered_set<string> obligatory_matched;

        for (auto it: args) {
            const string &name = it.first;
            if (obligatory_args.find(name) != obligatory_args.end()) {
                obligatory_matched.insert(name);
            } else {
                if (optional_args.find(name) == optional_args.end()) {
                    if (not_supported_help.tellp() == 0)
                        not_supported_help << "args not supported: " << name;
                    else
                        not_supported_help << ", " << name;
                }
            }
        }

        stringstream required_not_fount_help;
        if (obligatory_matched.size() != obligatory_args.size()) {
            for (auto it: obligatory_args) {
                const string &name = it.first;
                if (args.find(name) == args.end()) {
                    if (required_not_fount_help.tellp() == 0)
                        required_not_fount_help << "args required but not found: " << name;
                    else
                        required_not_fount_help << ", " << name;
                }
            }
        }

        if (not_supported_help.tellp() == 0) {
            if (required_not_fount_help.tellp() == 0)
                return "";
            else
                return HelpString() + " " + required_not_fount_help.str();
        } else {
            if (required_not_fount_help.tellp() == 0)
                return HelpString() + " " + not_supported_help.str();
            else
                return HelpString() + " " + required_not_fount_help.str() + " " + not_supported_help.str();
        }
    }

    string HelpString() {
        stringstream help;
        if (!obligatory_args.empty()) {
            help << "args required:";
            for (auto it: obligatory_args) {
                const ExpectedArg &arg = it.second;
                help << " {\"" << arg.name << "\"(" << arg.type << "): " << arg.help << "}";
            }
        }
        if (!optional_args.empty()) {
            help << "args required:";
            for (auto it: optional_args) {
                const ExpectedArg &arg = it.second;
                help << " {\"" << arg.name << "\"(" << arg.type << "): " << arg.help << "}";
            }
        }
        return (help.tellp() == 0) ? "no args required." : help.str();
    }

private:
    unordered_map<string, ExpectedArg> obligatory_args;
    unordered_map<string, ExpectedArg> optional_args;
};

}
