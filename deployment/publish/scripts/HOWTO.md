# How to deploy a developing cluster step by step with this toolbox
* Setup a compiling server
    * Find a server
    * Checkout TiSpark repo, build with mvn (for mvn building CHSpark)
    * Install all necessary libs use in TheFlash compiling
        * Libboost-devel ( `>` 1.6), for Decimal
        * Libtermcap-devel and libreadline, for client history commands
    * Checkout TheFlash repo, fetch all submodules, download Spark, build all modules
        * `benchmark/tpch-dbgen> make`
        * `storage> ./patch-apply.sh && ./build.sh`
        * `computing> ./build.sh`
* Publish a package
    * Update all repos, build them as above
    * `deployment/publish> ./publish.sh`: will generate a `theflash-{git-hash-short}.tar.gz` file
* Deploy a cluster
    * Cluster nodes: H0-Hn(ip addresses: IP0-IPn)
    * Use one node as ops-node: H0(ip address: IP0)
    * Make sure H0 can access any nodes(include itself) with ssh/scp without password, it's for:
        * `ceph-deploy` tool
        * `storages-*.sh` scripts
    * Copy `theflash-<git-hash-short>.tar.gz` to all nodes
        * Put it in the same file system path in all nodes
        * Unzip it: `theflash-{git-hash-short}> tar -xvzf <file>`
        * A good practice:
            * Copy and unzip the package to one node, then scp it to other nodes
            * `theflash-{git-hash-short}> ./storages-spread-file.sh` can help, as long as `_env.sh` is configed
    * Setup hosts file on every node, it's for ceph-deploy:
```
/etc/hosts: add these lines before the "127.0.0.1" line
IP0 H0
IP1 H1
IP2 H2
...
```
* Checkout cluster hardware envonment
    * Checkout CPU and memory: `theflash-{git-hash-short}> ./hw-report.sh`
    * Checkout disk IO, on each node:
        * `df -lh`: list mounted disk
        * `sudo fdisk -l`: list all disk (include unmounted) if necessary
        * `sudo yum install fio -y`: make sure fio installed
        * `theflash-{git-hash-short}> ./io-report.sh test-file-on-target-disk`:
            * Checkout disk IOPS and brandwidth
            * If the node has n diskes, run script n times to checkout each disk
    * Checkout network, on each nodes pair node-a and node-b:
        * Checkout network RTT: on node-a run command `ping node-b`
        * Checkout network brandwidth:
            * `sudo yum install iperf -y`: make sure iperf installed on all nodes
            * If iperf not found on repo source, install iperf3 instead
            * On node-a, run comand `iperf -s`
            * On node-b, run comand `iperf -c node-a`
* Deploy ceph if it's needed
    * Spare a raw disk for ceph osd(s)
        * Run `sudo umount disk-mounted-path` on all ceph nodes: umount the disk from filesystem
        * NOTICE: all data on it will be lost
    * Config ceph deploy args
        * In `theflash-{git-hash-short}>ceph-tools/deploy/_env.sh`:
            * `nodes=("H0" "H1" "H2" ...)`: ceph nodes should be a subset of all nodes
            * `public_network={network/mask}`
            * `user="{deploy user witch has sudo privilege}"`
            * `osd_img=""`
            * `dev_name="{the disk path (/dev/...) we spare for ceph osd}"`: the disk path on each node should be the same
    * Deploy ceph
        * Install `ceph-deploy`
            * Run all install scripts: `theflash-{git-hash-short}/ceph-tools/install/install-{os-type}.sh`
            * Run `ceph-deploy`, make sure it works
        * `theflash-{git-hash-short}/ceph-tools/deploy> ./deploy.sh`: deploy step by step with comfirmation
        * `theflash-{git-hash-short}/ceph-tools/deploy> ./status.sh`:
            * The first 3 nodes should have `mon + admin + mds + ods` running
            * The other nodes should have `ods` running
        * `theflash-{git-hash-short}/ceph-tools/deploy> ./create-fs.sh`: create ceph file system
        * Backup files, it's important for ceph ops: `theflash-{git-hash-short}>ceph-tools/deploy/ceph-*`
    * Create storage paths on ceph file system: (TODO: create dirs without mounting)
        * `theflash-{git-hash-short}/ceph-tools/deploy> sudo ./cephfs-mount.sh {some-dir} /`: mount root ceph file system
        * `mkdir {some-dir}/storage-0 && mkdir {some-dir}/storage-1 && ...`: create storage paths for each storage node
        * `theflash-{git-hash-short}/ceph-tools/deploy> sudo ./cephfs-umount.sh {some-dir}`: unmount ceph file system
    * Mount cephfs for storage, on each node:
        * `mkdir {some-storage-dir}`: create mount point dir, if `sudo mkdir` is used, don't forget `sudo chown`
        * `theflash-{git-hash-short}/ceph-tools/deploy> sudo ./cephfs-mount.sh {some-storage-dir} /storage-n`:
            * Mount ceph file system
            * Note that we mount different ceph file paths to the corresponding nodes
        * `theflash-{git-hash-short}> ./io-report.sh {some-storage-dir}/fio-test`: checkout mounted disk IO performance
    * Checkout ceph cluster status:
        * `sudo ceph -s` (TODO: remove `sudo` privilege)
* Deploy storage on nodes. On one node:
    * `theflash-{git-hash-short}> vim _env.sh`:
        * Config storage nodes: `export storage_server=("H0" "H1" ...)`
        * storage nodes should be a subset of all nodes
    * `theflash-{git-hash-short}> ./storages-spread-file.sh ./_env.sh`:
        * Spread (copy) `_env.sh` to all storage nodes
    * `theflash-{git-hash-short}> vim storage/config.xml`:
        * Modify data path (`<data> element`) to `{some-storage-dir}` (mounted as above)
        * Modify other paths to the correct ones, note that: do NOT set to a path located on system disk
    * `theflash-{git-hash-short}> ./storages-spread-file.sh storage/config.xml`:
        * Sync config to all storage nodes
        * Each time we make some changes, sync config juse like this
    * `theflash-{git-hash-short}> ./storages-dsh.sh "./storage-server.sh &"`:
        * Bring all storages online
        * Manually run `storage-server.sh` on each node if we like
    * `theflash-{git-hash-short}> ./storages-pid.sh`: checkout storage pid(s)
    * `theflash-{git-hash-short}> ./storage-client.sh "show database"`: checkout storage cluster status
* Run Spark. On Spark master node (can be any one of all nodes):
    * `theflash-{git-hash-short}> vim _env.sh`:
        * Config Spark master: `export spark_master=""`
        * Sync `_env.sh` to all storage nodes as above
            * TODO: support `Spark nodes` != `storage nodes`, now they should the same
    * `theflash-{git-hash-short}> ./spark-start-master.sh`: bring master online
    * `theflash-{git-hash-short}> ./storages-dsk.sh "./spark-start-slave.sh"`: bring slaves online
    * `theflash-{git-hash-short}> ./spark-check-running.sh`: checkout Spark cluster status
* TCP-H test. On one node:
    * `theflash-{git-hash-short}> ./load-all.sh`: generate and load data
    * `theflash-{git-hash-short}/tcph/load> vim _env.sh`:
        * Config storage nodes: `export nodes=("H0" "H1" ...)`
        * Config data scale and other options
    * `theflash-{git-hash-short}/tcph/load> ./load-all.sh`: generate and load data to storage cluster
    * `theflash-{git-hash-short}/tcph/load> ./count-all.sh true`: show tables row count on cluster
