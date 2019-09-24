# TiFlash integration test tools
## basic shell
- _cluster_env.sh: contain environment variables
- _luster_helper.sh: contain auxiliary function
- cluster_deploy.sh: deploy cluster to target machine
- cluster_start.sh: start cluster
- cluster_stop.sh: stop cluster
- cluster_cleanup_data.sh: clean up cluster data
- cluster_unsafe_destroy.sh: destroy cluster
- cluster_check_result_consistency.sh: check whether tiflash's count result is consistent with tidb's
- cluster_wait_data_ready.sh: wait for loading data completed
- cluster_delete_single_tiflash.sh: delete a tiflash node from the cluster
- cluster_dsh.sh: dsh file
- cluster_spread_file.sh: copy file to every node in the cluster
- cluster_mysql_client.sh: connect to a tidb server in the cluster
- cluster_insert_sample_data.sh: insert sample data to the cluster
- config_ssh.sh: configure free login
- load_data/: generate and load data into cluster


## test shell(every test starts with prefix 'nightly_')
- nightly_restart_test.sh: restart tiflash test
- nightly_expand_shrink_test.sh: expand and shrink cluster test
