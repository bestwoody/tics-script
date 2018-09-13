import os
import sys

def extract_name(line):
	line = line.strip()
	i = line.find('(')
	if i > 0:
		line = line[:i].strip()
	return line.split()[0]

def parse_dups(path):
	file = open(path)
	lines = file.readlines();
	lines = filter(lambda x: x.find('sub deplist ') < 0, lines)
	lines = map(lambda x: extract_name(x[:-1]), lines)
	file.close()
	return set(lines)

def parse_syslibs(path):
	file = open(path)
	lines = file.readlines();
	lines = map(lambda x: x[:-1], lines)
	file.close()
	return lines

def parse_deps(dups, syslibs):
	dep_prefix = 'dependency: '

	while True:
		line = sys.stdin.readline()
		if not line:
			break
		line = line.strip()
		if len(line) == 0:
			continue
		if not line.startswith(dep_prefix):
			continue

		line = line[len(dep_prefix):]

		if line.startswith('/'):
			continue
		if (line.startswith('lib') or line.startswith('ld-')) and line.find('.so') > 0:
			continue

		is_syslib = False
		for lib in syslibs:
			if line.startswith(lib):
				is_syslib = True
				break
		if is_syslib:
			continue

		line = extract_name(line)

		if line in dups:
			continue
		dups.add(line)

		print line

def parse():
	if len(sys.argv) < 3:
		sys.stderr.write("usage: <bin> extracted-libs-list-file system-libs-list-file\n")
		sys.exit(1)

	dups = parse_dups(sys.argv[1])
	syslibs = parse_syslibs(sys.argv[2])
	parse_deps(dups, syslibs)

parse()
