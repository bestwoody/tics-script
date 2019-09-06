# The `to_table` auto report-generating tool
This tool is designed for recording data(eg: elapsed times of TPCH benchmark) in testing, and auto generating report from recorded data easily.


## How to record data
The recorded data should have many lines, each line would be:
```
value tag:xxx,tag:xxx,...
```
The value should be a number(int or long).
The related tag set indicate what is the meaning of this value.

A simple sample:
```
28 op:stop,ts:1567737979,mod:tiflash,ver:0.1.1,git:71e1b04282c18ab51652cec12e8c010e2abc89d2
```
This was recorded in bash script when stopping a TiFlash instance, the value `28` means it toke 28s to stop.

You can record data to file in this format easily(eg: use `echo` in bash).


## How to generate report table
Assume that we recorded data to file `my.data`:
```
341 op:run,ts:1567740928,mod:tidb,ver:v3.0.2,git:94498e7d06a244196bb41c3a05dd4c1f6903099a
234236 op:run,ts:1567740934,mod:tiflash,ver:0.1.1,git:71e1b04282c18ab51652cec12e8c010e2abc89d2
23424 op:run,ts:1567740938,mod:rngine,ver:3.0.0-alpha,git:96a94433633a06ca8a2afbb9b41325454cad671f
221 op:stop,ts:1567740940,mod:pd,ver:v3.0.0-alpha-152-gb65cecd4,git:b65cecd4e5010e61142a8bcb8a6b9220def6aca9
341 op:stop,ts:1567740941,mod:tikv,ver:3.0.2,git:2342831c3b9e218fcd6142dd1ec1f5996d98cbb5
330 op:stop,ts:1567740942,mod:tidb,ver:v3.0.2,git:94498e7d06a244196bb41c3a05dd4c1f6903099a
133 op:stop,ts:1567740943,mod:tiflash,ver:0.1.1,git:71e1b04282c18ab51652cec12e8c010e2abc89d2
22 op:stop,ts:1567740943,mod:rngine,ver:3.0.0-alpha,git:96a94433633a06ca8a2afbb9b41325454cad671f
33240 op:run,ts:1567740952,mod:pd,ver:v3.0.0-alpha-152-gb65cecd4,git:b65cecd4e5010e61142a8bcb8a6b9220def6aca9
144 op:run,ts:1567740954,mod:tikv,ver:3.0.2,git:2342831c3b9e218fcd6142dd1ec1f5996d98cbb5
2 op:run,ts:1567740955,mod:tidb,ver:v3.0.2,git:94498e7d06a244196bb41c3a05dd4c1f6903099a
6442 op:run,ts:1567740961,mod:tiflash,ver:0.1.1,git:71e1b04282c18ab51652cec12e8c010e2abc89d2
```

Use command `totable 'cols:op; rows:mod,ver; cell:avg' 9999 my.data`, we got a table:
```
                                           | op:run | op:stop |
mod:tidb    ver:v3.0.2                     |    171 |     330 |
mod:tiflash ver:0.1.1                      | 120339 |     133 |
mod:rngine  ver:3.0.0-alpha                |  23424 |      22 |
mod:pd      ver:v3.0.0-alpha-152-gb65cecd4 |  33240 |     221 |
mod:tikv    ver:3.0.2                      |    144 |     341 |
```

Since this is about elapsed times, we add `duration` operator to each cell:
Use command `totable 'cols:op; rows:mod,ver; cell:avg:duration' 9999 my.data`, we got:
```
                                           | op:run | op:stop |
mod:tidb    ver:v3.0.2                     |   171s |    330s |
mod:tiflash ver:0.1.1                      |    33h |    133s |
mod:rngine  ver:3.0.0-alpha                |   390m |     22s |
mod:pd      ver:v3.0.0-alpha-152-gb65cecd4 |   554m |    221s |
mod:tikv    ver:3.0.2                      |   144s |    341s |
```

We can use `cell:avg:~` to show how stable is it the number of each cell:
```
                                           |          op:run | op:stop |
mod:tidb    ver:v3.0.2                     |       +-170 171 |     330 |
mod:tiflash ver:0.1.1                      | +-113897 120339 |     133 |
mod:rngine  ver:3.0.0-alpha                |           23424 |      22 |
mod:pd      ver:v3.0.0-alpha-152-gb65cecd4 |           33240 |     221 |
mod:tikv    ver:3.0.2                      |             144 |     341 |
```

Or, we can switch row-col using `to_table '' 'rows:op; cols:mod; cell:bytes' 9999 my.data`:
```
        | mod:tidb | mod:tiflash | mod:rngine | mod:pd | mod:tikv |
op:run  |     171b |        117k |        22k |    32k |     144b |
op:stop |     330b |        133b |        22b |   221b |     341b |
```

# The detail usage of `to_table`
You could use `to_table.py` or bash function `to_table`, the args are:
```
usage: <bin> table_title, render_str, tail_limit_on_each_file, data_file1, [data_file2] [...]
```
The `render_str` is the most important one, the others are plain to understand.

The `render_str` format:
```
pre-process section; pre-process section; rows-defination section; cols-defination section; cell-process section
```
The `render_str` is made up by 0~many `pre-process section`, 1 `rows-defination`, 1 `cols-defination`, and 1 `cell-process section`.
Sections are seperated by `;`, the order of the sections are irrelevant.

The `pre-process` sections will be applied on each line from input file, format:
```
op1:op2:op3;
```
In `pre-process` sections, we support operators as follow:
```
tag2=tag1            -- rename a tag, eg: we write `service=mod`, then we can use `service` as a valid tag in other sections.
tag2=to_month(tag1)  -- treat the value of tag1 as timestamp, and cast into month unit. it means `group by month`. WIP
tag2=to_day(tag1)    -- similer to `to_month`
```

The `rows-defination` section defines the table rows, format:
```
rows:tag1,tag2:op1:op2:op3;
```
The tag set here defines which tags are used as rows, can be 1~many tags.
The order of operators is relevent, operators are executed one by one.

In `rows-defination` sections, we support operators as follow:
```
notag                -- hide tag name from row titile. eg: `op:tikv,ver:3.0.0` => `tikv,3.0.0`
limit(n)             -- only show last `n` rows.
limit(n, tag1, tag2) -- only calculate last `n` rows containing specified tags. WIP
sort(tag1, tag2)     -- sort rows by tags' value. WIP
```

The `cell-process` sections format:
```
cell:op1:op2:op3;
```
In `cell-process` sections, we support operators as follow:
```
avg       -- each table cell contains data from many source lines, `avg` calculate the avg value of each line.
mid       -- calculate the mid value of each line. WIP
nobig     -- don't calulate the value witch is too big than others. WIP
nosmall   -- don't calulate the value witch is too small than others. WIP
noextre   -- don't calulate the value witch is too big or to small than others. WIP
~         -- shows the inflation of cell values, only could be used on number, so it can't follow `duration` or `bytes`.
cnt       -- shows count of lines in this cell. WIP
duration  -- cast cell value into duration format, the unit could be 's|m|h|y'.
bytes     -- cast cell value into bytes format, the unit could be 'b|k|m|g'.
```

The `cols-defination` section is almost the same as `rows-defination`.

END
