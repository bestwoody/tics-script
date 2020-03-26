def runReleaseIntegrationTest(branch, version, maxRunTime, notify) {
  runReleaseIntegrationTest(branch, version, "", "", "", "", maxRunTime, notify)
}

def runReleaseIntegrationTest(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, maxRunTime, notify) {
    taskStartTimeInMillis = System.currentTimeMillis()

    def label = "tiflash-release-test-v11"

    podTemplate(name: label, label: label, instanceCap: 10, idleMinutes: 5, containers: [
            containerTemplate(name: 'tiflash-docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'docker-ops-ci', image: 'hub.pingcap.net/tiflash/ops-ci:v11',
                    resourceRequestCpu: '10000m',
                    resourceRequestMemory: '20Gi',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
    ]) {
        catchError {
            node(label) {
                stage("Download Resources") {
                    container("tiflash-docker") {
                        dir("/home/jenkins/agent/git/tiflash/") {
                            sh "chown -R 1000:1000 ./"

                            def ws = pwd()
                            deleteDir()

                            checkout changelog: false, poll: false, scm: [
                                    $class                           : 'GitSCM',
                                    branches                         : [[name: branch]],
                                    doGenerateSubmoduleConfigurations: false,
                                    userRemoteConfigs                : [[credentialsId: 'github-sre-bot-ssh', url: 'git@github.com:pingcap/tiflash.git']]
                            ]

                            dailyTest = load 'regression_test/daily.groovy'
                            tidbTest = load 'regression_test/tidb_test.groovy'
                            schrodingerTest = load 'regression_test/schrodinger.groovy'
                        }
                    }
                }
            }
        }

        parallel(
                // Daily Test
                "Daily Test": {
                    dailyTest.runDailyIntegrationTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, notify)
                },
                // TiDB Test
                "TiDB Test": {
                    tidbTest.runTiDBTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, notify)
                },
                // Schrodinger
                "schrodinger/bank Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank", maxRunTime, notify)
                },
                "schrodinger/bank2 Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank2", maxRunTime, notify)
                },
                "schrodinger/crud Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/crud", maxRunTime, notify)
                },
                "schrodinger/ledger Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/ledger", maxRunTime, notify)
                },
                "schrodinger/ledger Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/sqllogic", maxRunTime, notify)
                },
                "schrodinger/ddl Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/ddl", maxRunTime, notify)
                },
                // Region Merge
                "schrodinger/bank Region Merge Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank true", maxRunTime, notify)
                },
                "schrodinger/bank2 Region Merge Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank2 true", maxRunTime, notify)
                },
                "schrodinger/crud Region Merge Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/crud true", maxRunTime, notify)
                },
                "schrodinger/ledger Region Merge Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/ledger true", maxRunTime, notify)
                },
                // Pessimistic
                "schrodinger/bank Pessimistic Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank false false false true", maxRunTime, notify)
                },
                "schrodinger/bank2 Pessimistic Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank2 false false false true", maxRunTime, notify)
                },
                "schrodinger/crud Pessimistic Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/crud false false false true", maxRunTime, notify)
                },
                "schrodinger/ledger Pessimistic Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/ledger false false false true", maxRunTime, notify)
                },
                // Follower Read
                "schrodinger/bank Follower Read Test": {
                    schrodingerTest.runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, "schrodinger/bank false false false false true", maxRunTime, notify)
                }
        )
    }
}

return this
