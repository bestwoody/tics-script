import sys
import subprocess

ti = sys.argv[1]
fi = sys.argv[2]
index = sys.argv[3]

create_stmt = "create table test.%s (a int);"
insert_stmt = "insert into test.%s values %s;"

case_num = int(sys.argv[4])
insert_num = int(sys.argv[5])

def cmdMustRun(cmd):
    print "cmd: ", cmd
    result = subprocess.check_output("bash " + cmd, shell=True)
    if "Exception" in result:
        sys.exit(1)
    return result

def sqlRun(stmt, engine):
    global ti, fi
    cmd = "%s -i %s %s %s \"%s\"" % (ti, index, fi, engine, stmt)
    return cmdMustRun(cmd)

for i in range(case_num):
    name  = ("t_" + str(i))
    create = create_stmt % (name)
    sqlRun(create, "mysql")
    values = "(0)"
    for j in range(insert_num-1):
        values += ", (%d)" % (j+1)
    insert = insert_stmt % (name,values)
    sqlRun(insert, "mysql")

for i in range(case_num):
    name  = ("t_" + str(i))
    alter = "alter table test.%s set tiflash replica 1" % (name)
    sqlRun(alter, "mysql");

for i in range(case_num):
    name  = ("t_" + str(i))
    query = "select /*+ read_from_storage(tiflash[test.%s]) */ count(*) from test.%s;" % (name, name);
    result = sqlRun(query, "mysql");
    print result
    result_arr = result.split('\n')
    if result_arr[0] != 'count(*)' and result_arr[1] != '10':
        print 'error result : ', result_arr
        print 'expected result : ', result
        exit(1)

