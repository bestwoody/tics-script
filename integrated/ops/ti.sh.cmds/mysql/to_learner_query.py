# -*- coding:utf-8 -*-

import sys

def rewrite_one(query, origin_has_select):
    j = query.find('from')
    if j < 0:
        if origin_has_select:
            return 'select ' + query
        else:
            return query
    tail = query[j + 5:]

    tables = None
    last = None
    found = False
    for i in range(0, len(tail)):
        c = tail[i]
        if c == ' ' or c == '\t':
            if last != ' ' and last != '\t' and last != ',':
                tables = tail[0:i]
                found = True
                break
        last = c
        index = i

    if not found:
        tables = tail
    if not tables:
        return 'select ' + query
    return 'select /*+ read_from_storage(tiflash[' + tables + ']) */ ' + query

def rewrite(origin):
    query = origin.lower().strip()
    if query.find('select') < 0:
        return origin
    starts_with_select = query.startswith('select')
    subs = query.split('select')
    subs = map(lambda x: x.strip(), subs)
    subs = filter(lambda x: x, subs)
    for i in range(0, len(subs)):
        subs[i] = rewrite_one(subs[i], i != 0 or starts_with_select)
    return ' '.join(subs)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print '[to_learner_query.py] usage: <bin> query'
        sys.exit(1)
    print rewrite(sys.argv[1])
