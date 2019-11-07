def runDailyIntegrationTest(branch, label) {
    taskStartTimeInMillis = System.currentTimeMillis()

    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"

    podTemplate(name: label, label: label, instanceCap: 5, idleMinutes: 5, containers: [
            containerTemplate(name: 'tiflash-docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'docker-ops-ci', image: 'hub.pingcap.net/tiflash/ops-ci:v10',
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
                        dir("/home/jenkins/agent/git/tiflash/binary/") {
                            sh "chown -R 1000:1000 ./"

                            def ws = pwd()
                            deleteDir()

                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }

                            checkout changelog: false, poll: false, scm: [
                                    $class                           : 'GitSCM',
                                    branches                         : [[name: branch]],
                                    doGenerateSubmoduleConfigurations: false,
                                    userRemoteConfigs                : [[credentialsId: 'github-sre-bot-ssh', url: 'git@github.com:pingcap/tiflash.git']]
                            ]

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

                            ls bin/tidb-server
                            ls bin/tikv-server
                            ls bin/pd-server
                            ls tics/theflash
                            """
                        }
                    }
                }

                stage("Regression Test") {
                    container("docker-ops-ci") {
                        dir("/home/jenkins/agent/git/tiflash") {
                            sh "regression_test/daily.sh"
                        }
                    }
                }
            }
            currentBuild.result = "SUCCESS"
        }
    }

    stage('Summary') {
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

return this
