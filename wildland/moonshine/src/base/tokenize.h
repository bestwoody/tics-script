#pragma once

#include "base/err.h"

namespace moonshine {

// TODO: a very simple parser, should be rewrited
class TokenParser {
public:
    TokenParser(const char *source_, bool skip_blank_ = true)
        : curr(source_), last_parsed(curr), skip_blank(skip_blank_) {
        SkipBlanks();
    }

    void Assign(const char *source) {
        curr = source;
        SkipBlanks();
    }

    char Peek() {
        return *curr;
    }

    const char * Unparsed() {
        return curr;
    }

    const char * LastParsed() {
        return last_parsed;
    }

    bool Finished() {
        return !*curr;
    }

    size_t SkipBlanks() {
        if (!skip_blank)
            return 0;
        if (!*curr)
            return 0;
        const char *source = curr;
        while (*curr && (*curr == ' ' || *curr == '\t'))
            ++curr;
        return curr - source;
    }

    bool MatchToken(string &token) {
        last_parsed = curr;
        while (*curr && IsTokenChar(*curr))
            ++curr;
        if (last_parsed == curr)
            return false;
        token.assign(last_parsed, curr - last_parsed);
        SkipBlanks();
        return true;
    }

    bool MatchChar(char c, bool ignore_case = false) {
        if (!Equal(*curr, c, ignore_case))
            return false;
        last_parsed = curr;
        ++curr;
        SkipBlanks();
        return true;
    }

    bool MatchString(const char *str, bool ignore_case = false) {
        last_parsed = curr;
        while (*curr && *str && Equal(*curr, *str, ignore_case)) {
            ++curr;
            ++str;
        }
        if (*str)
            return false;
        SkipBlanks();
        return true;
    }

    static bool IsLowerChar(char c) {
        return 'a' <= c && c <= 'z';
    }

    static bool IsUpperChar(char c) {
        return 'A' <= c && c <= 'Z';
    }

    static bool IsDigitChar(char c) {
        return '0' <= c && c <= '9';
    }

    static bool IsTokenChar(char c) {
        return IsLowerChar(c) || IsUpperChar(c) || IsDigitChar(c) || c == '_';
    }

    static bool Equal(char a, char b, bool ignore_case) {
        return (ignore_case) ? (a == b) : EqualIgnoreCase(a, b);
    }

    static bool EqualIgnoreCase(char a, char b) {
        return a == b || (a <= 'Z' && (a + ('z' - 'Z') == b)) || (a - ('z' - 'Z') == b);
    }

private:
    const char *curr;
    const char *last_parsed;
    bool skip_blank;
};

}
