import sys
import subprocess

ti = sys.argv[1]
fi = sys.argv[2]

insert_num = int(sys.argv[3])
batch = int(sys.argv[4])

create_stmt = "create table test.t (a int) shard_row_id_bits = 4;"
insert_stmt = "insert into test.%s values %s;"

def cmdMustRun(cmd):
    result = subprocess.check_output("bash " + cmd, shell=True)
    if "Exception" in result:
        sys.exit(1)
    return result

def sqlRun(stmt, engine):
    global ti, fi
    cmd = "%s %s %s \"%s\"" % (ti, fi, engine, stmt)
    return cmdMustRun(cmd)

sqlRun(create_stmt, "mysql")
alter = "alter table test.t set tiflash replica 1"
sqlRun(alter, "mysql");


for k in range(insert_num):
    values = ''
    for j in range(batch):
        values += "(%d)" % (j+1+k * batch)
        if j != batch - 1:
            values += ','
    insert = insert_stmt % ("t",values)
    if k % 100 == 0 and k != 0:
        print "stmt ", insert
    sqlRun(insert, "mysql")

query1 = "select /*+ read_from_storage(tiflash[test.t]) */ count(*) from test.t"
result = sqlRun(query1, "mysql")
result_arr = result.split('\n')
if result_arr[0] != 'count(*)' and int(result_arr[1]) != batch * insert_num:
    print 'error result : ', result_arr
    print 'expected result : ', result
    exit(1)
query2 = "select /*+ read_from_storage(tiflash[test.t]) */ * from test.t"
result = sqlRun(query2, "mysql")
result_arr = result.split('\n')
total = batch * insert_num
continuous = True
s = 0
for i, value in enumerate(result_arr[1:-2]):
    if int(value) != i+1:
        continous = False
    s += int(value)

if continous:
    print 'write balance does not work!!'
    print result_arr
    exit(1)
expected_sum = (1 + total) * total / 2
if s != expected_sum:
    print 'error: expected sum is :', expected_sum, ' but real sum is', s
    print result_arr
    exit(1)
