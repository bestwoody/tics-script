def runTest(branch, label, notify) {
    taskStartTimeInMillis = System.currentTimeMillis()

    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"

    podTemplate(name: label, label: label, instanceCap: 5, idleMinutes: 5, containers: [
            containerTemplate(name: 'tiflash-docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'docker-ops-ci', image: 'hub.pingcap.net/tiflash/ops-ci:v11',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
    ]) {
        catchError {
            node(label) {
                stage("Clean") {
                    container("tiflash-docker") {
                        sh """
                        killall -9 tidb-server || true
                        killall -9 tikv-server || true
                        killall -9 pd-server || true
                        killall -9 theflash || true
                        killall -9 tikv-server-rngine || true
                        pkill -f 'java*' || true
                        """
                    }
                }

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
                        }

                        dir("/home/jenkins/agent/git/tiflash/binary/") {
                            // tidb
                            def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                            // tikv
                            def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                            // pd
                            def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"

                            sh """
                            chown -R 1000:1000 ./
                            ID=\$(docker create hub.pingcap.net/tiflash/tics:master)
                            docker cp \${ID}:/tics ./
                            docker rm \${ID}
                            chown -R 1000:1000 ./
                            """
                        }
                    }
                }

                stage("Test Tiflash Latest Stable") {
                    container("docker-ops-ci") {
                        dir("/home/jenkins/agent/git/tiflash") {
                            try {
                                timeout(360) {
                                    sh "regression_test/daily.sh"
                                }
                            } catch (err) {
                                sh "cat /tmp/ti/ci/release/rngine/rngine.log"
                                sh "cat /tmp/ti/ci/release/rngine/rngine_stderr.log"
                                sh "cat /tmp/ti/ci/release/tiflash/tmp/flash_cluster_manager.log"
                                sh "cat /tmp/ti/ci/release/tiflash/log/error.log"
                                sh "cat /tmp/ti/ci/release/tiflash/log/server.log"
                                sh "cat /tmp/ti/ci/release/pd/pd_stderr.log"
                                sh "cat /tmp/ti/ci/release/pd/pd.log"
                                sh "cat /tmp/ti/ci/release/tidb/tidb.log"
                                sh "cat /tmp/ti/ci/release/tidb/tidb_stderr.log"
                                sh "cat /tmp/ti/ci/release/tikv/tikv_stderr.log"
                                sh "cat /tmp/ti/ci/release/tikv/tikv.log"

                                sh "cat /tmp/ti/ci/self/rngine/rngine.log"
                                sh "cat /tmp/ti/ci/self/rngine/rngine_stderr.log"
                                sh "cat /tmp/ti/ci/self/tiflash/tmp/flash_cluster_manager.log"
                                sh "cat /tmp/ti/ci/self/tiflash/log/error.log"
                                sh "cat /tmp/ti/ci/self/tiflash/log/server.log"
                                sh "cat /tmp/ti/ci/self/pd/pd_stderr.log"
                                sh "cat /tmp/ti/ci/self/pd/pd.log"
                                sh "cat /tmp/ti/ci/self/tidb/tidb.log"
                                sh "cat /tmp/ti/ci/self/tidb/tidb_stderr.log"
                                sh "cat /tmp/ti/ci/self/tikv/tikv_stderr.log"
                                sh "cat /tmp/ti/ci/self/tikv/tikv.log"

                                throw err
                            }
                        }
                    }
                }

                stage("Test Tiflash Master Branch") {
                    container("docker-ops-ci") {
                        dir("/home/jenkins/agent/git/tiflash") {
                            try {
                                sh "cp regression_test/conf/bin.paths integrated/conf/"
                                timeout(360) {
                                    sh "regression_test/daily.sh"
                                }
                            } catch (err) {
                                sh "cat /tmp/ti/ci/release/rngine/rngine.log"
                                sh "cat /tmp/ti/ci/release/rngine/rngine_stderr.log"
                                sh "cat /tmp/ti/ci/release/tiflash/tmp/flash_cluster_manager.log"
                                sh "cat /tmp/ti/ci/release/tiflash/log/error.log"
                                sh "cat /tmp/ti/ci/release/tiflash/log/server.log"
                                sh "cat /tmp/ti/ci/release/pd/pd_stderr.log"
                                sh "cat /tmp/ti/ci/release/pd/pd.log"
                                sh "cat /tmp/ti/ci/release/tidb/tidb.log"
                                sh "cat /tmp/ti/ci/release/tidb/tidb_stderr.log"
                                sh "cat /tmp/ti/ci/release/tikv/tikv_stderr.log"
                                sh "cat /tmp/ti/ci/release/tikv/tikv.log"

                                sh "cat /tmp/ti/ci/self/rngine/rngine.log"
                                sh "cat /tmp/ti/ci/self/rngine/rngine_stderr.log"
                                sh "cat /tmp/ti/ci/self/tiflash/tmp/flash_cluster_manager.log"
                                sh "cat /tmp/ti/ci/self/tiflash/log/error.log"
                                sh "cat /tmp/ti/ci/self/tiflash/log/server.log"
                                sh "cat /tmp/ti/ci/self/pd/pd_stderr.log"
                                sh "cat /tmp/ti/ci/self/pd/pd.log"
                                sh "cat /tmp/ti/ci/self/tidb/tidb.log"
                                sh "cat /tmp/ti/ci/self/tidb/tidb_stderr.log"
                                sh "cat /tmp/ti/ci/self/tikv/tikv_stderr.log"
                                sh "cat /tmp/ti/ci/self/tikv/tikv.log"

                                throw err
                            }
                        }
                    }
                }
            }
            currentBuild.result = "SUCCESS"
        }
    }

    stage('Summary') {
        if (notify == "true" || notify == true) {
            def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
            def slackmsg = "TiFlash Daily Integration Test\n" +
                    "Result: `${currentBuild.result}`\n" +
                    "Elapsed Time: `${duration}` Mins\n" +
                    "https://internal.pingcap.net/idc-jenkins/blue/organizations/jenkins/tiflash_regression_test_daily/activity\n" +
                    "https://internal.pingcap.net/idc-jenkins/job/tiflash_regression_test_daily/"

            if (currentBuild.result != "SUCCESS") {
                slackSend channel: '#tiflash-daily-test', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            } else {
                slackSend channel: '#tiflash-daily-test', color: 'good', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            }
        }
    }
}

def runDailyIntegrationTest(branch, label, notify) {
    runTest(branch, label, notify)
}

def runDailyIntegrationTest(branch, label) {
    runTest(branch, label, false)
}

return this
