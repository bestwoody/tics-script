def runSchordingerTest(branch, version, testcase, maxRunTime, notify) {
    taskStartTimeInMillis = System.currentTimeMillis()

    def label = "test-tiflash-schordinger-v11"

    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"
    def TIFLASH_BRANCH = "master"

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
                        killall -9 tiflash || true
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

                        if(version == "latest") {
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

                              // tiflash
                              def tiflash_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tiflash/${TIFLASH_BRANCH}/sha1").trim()
                              sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tiflash/${TIFLASH_BRANCH}/${tiflash_sha1}/centos7/tiflash.tar.gz | tar xz"

                              sh """
                              cd tiflash
                              tar -zcvf flash_cluster_manager.tgz flash_cluster_manager/
                              """
                            }
                        }
                    }
                }

                stage("Test_" + branch + "_" + version + "_"  + testcase) {
                    container("docker-ops-ci") {
                        dir("/home/jenkins/agent/git/tiflash") {
                            if(version == "latest") {
                              sh "rm -f integrated/conf/bin.paths"
                              sh "cp regression_test/conf/bin.paths integrated/conf/"
                            }

                            def startTime = System.currentTimeMillis()
                            try {
                                timeout(maxRunTime) {
                                    sh "regression_test/schordinger.sh " + testcase
                                }
                            } catch (err) {
                                def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(0, BigDecimal.ROUND_HALF_UP)
                                if (duration < Integer.parseInt(maxRunTime)) {
                                    sh "for f in \$(find . -name '*.log'); do echo \"LOG: \$f\"; tail -500 \$f; done"
                                    throw err
                                }
                            }
                        }
                    }
                }
            }
            currentBuild.result = "SUCCESS"
        }
    }

    stage('Summary') {
      def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
      def slackmsg = "TiFlash Schordinger Test\n" +
              "Branch: `${branch}`\n" +
              "Version: `${version}`\n" +
              "Testcase: `${testcase}`\n" +
              "Result: `${currentBuild.result}`\n" +
              "Elapsed Time: `${duration}` Mins\n" +
              "https://internal.pingcap.net/idc-jenkins/blue/organizations/jenkins/tiflash_schordinger_test/activity\n" +
              "https://internal.pingcap.net/idc-jenkins/job/tiflash_schordinger_test/"
        print slackmsg
        if (notify == "true" || notify == true) {
            if (currentBuild.result != "SUCCESS") {
                slackSend channel: '#tiflash-daily-test', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            } else {
                slackSend channel: '#tiflash-daily-test', color: 'good', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            }

        }
    }
}

return this
