import sys

def run():
    def isTitle(line):
        return line.find('tpch query #') >= 0;

    def isScore(line):
        return line.find('elapsed: Double = ') >= 0

    def getScore(line):
        return float(line[len('elapsed: Double = '):])

    result = {}
        title = None

        while True:
            line = sys.stdin.readline()
                if not line:
                    break
                if len(line) == 0:
                    continue

                line = line[:-1]
                titleLine = isTitle(line)
                scoreLine = isScore(line)

                if not titleLine and not scoreLine:
                    continue

                if titleLine:
                    title = line[len('## Running tpch query #'):]
                        continue

                score = getScore(line)
                if result.has_key(title):
                    sum, count, array = result[title]
                        array.append(score)
                        result[title] = (sum + score, count + 1, array)
                else:
                    result[title] = (score, 1, [score])
                title = None	

        for k, v in result.iteritems():
            sum, count, array = v
                print k, sum / count, array

run()
