# The Flash regression test

The Flash regression test is triggered by [this jenkins task](https://internal.pingcap.net/idc-jenkins/job/tiflash_regression_test_daily/) every night.

The result of the Flash regression test will be sent to this slack channel: [#tiflash-daily-test](https://pingcap.slack.com/messages/CQ3EL6Q95).

## How to run regression on local environment?

1. Start docker:

```
cd ${tiflash_home_dir}
docker run --rm -i -t -v `pwd`:/tiflash --name ops-ci hub.pingcap.net/tiflash/ops-ci:v10 /bin/bash
```

2. Run regression test in docker:

```
/tiflash/regression_test/daily.sh
```

## Test tiflash on master branch

If you want to test tiflash on master branch, please download the binary first:

```
# make binary dir
cd ${tiflash_home_dir}
mkdir binary
cd binary

# download latest tiflash
ID=$(docker create hub.pingcap.net/tiflash/tics:master)
docker cp ${ID}:/tics ./
docker rm ${ID}

# download latest tidb
TIDB=$(curl http://fileserver.pingcap.net/download/refs/pingcap/tidb/master/sha1)
curl http://fileserver.pingcap.net/download/builds/pingcap/tidb/$TIDB/centos7/tidb-server.tar.gz | tar xz

# download latest tikv
TIKV=$(curl http://fileserver.pingcap.net/download/refs/pingcap/tikv/master/sha1)
curl http://fileserver.pingcap.net/download/builds/pingcap/tikv/$TIKV/centos7/tikv-server.tar.gz | tar xz
```

and config `integrated/conf/bin.paths`
```
cd ${tiflash_home_dir}
cp regression_test/conf/bin.paths integrated/conf/
```