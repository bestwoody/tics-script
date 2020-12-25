def runSchrodingerTest(branch, version, testcase, maxRunTime, notify) {
  runSchrodingerTest2(branch, version, "", "", "", "", testcase, maxRunTime, notify)
}

def runSchrodingerTest2(branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, testcase, maxRunTime, notify) {
  runSchrodingerTest3("kubernetes", branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, testcase, maxRunTime, notify)
}

def runSchrodingerTest3(cloud, branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, testcase, maxRunTime, notify) {
  runSchrodingerTest4(cloud, branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, testcase, maxRunTime, notify, 5)
}

def runSchrodingerTest4(cloud, branch, version, tidb_commit_hash, tikv_commit_hash, pd_commit_hash, tiflash_commit_hash, testcase, maxRunTime, notify, idleMinutes) {
    taskStartTimeInMillis = System.currentTimeMillis()

    def label = "test-tiflash-Schrodinger-v11"

    podTemplate(cloud: cloud, name: label, label: label, instanceCap: 20, idleMinutes: idleMinutes, containers: [
            containerTemplate(name: 'tiflash-docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'docker-ops-ci', image: 'hub.pingcap.net/tiflash/ops-ci:v11',
                    resourceRequestCpu: '5000m',
                    resourceRequestMemory: '10Gi',
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
                                    userRemoteConfigs                : [[credentialsId: 'github-sre-bot-ssh',refspec: '+refs/heads/*:refs/remotes/origin/* +refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/tiflash.git']]
                            ]

                            def TIDB_BRANCH = branch
                            def TIKV_BRANCH = branch
                            def PD_BRANCH = branch
                            def TIFLASH_BRANCH = branch

                            def ver_file = "regression_test/download_ver.ti"
                            sh "rm $ver_file"
                            if(version == "latest") {
                                dir("/home/jenkins/agent/git/tiflash/binary/") {
                                    // tidb
                                    def tidb_sha1 = tidb_commit_hash
                                    if (tidb_sha1 == null || tidb_sha1 == "") {
                                        tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                                    }

                                    // tikv
                                    def tikv_sha1 = tikv_commit_hash
                                    if (tikv_sha1 == null || tikv_sha1 == "") {
                                        tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                                    }

                                    // pd
                                    def pd_sha1 = pd_commit_hash
                                    if (pd_sha1 == null || pd_sha1 == "") {
                                        pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                                    }

                                    // tiflash
                                    def tiflash_sha1 = tiflash_commit_hash
                                    if (tiflash_sha1 == null || tiflash_sha1 == "") {
                                        tiflash_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tiflash/${TIFLASH_BRANCH}/sha1").trim()
                                    }
                                    sh """
                                    echo \"ver=''\"                          > $ver_file
                                    echo \"tidb_branch=$TIDB_BRANCH\"       >> $ver_file
                                    echo \"tidb_hash=$tidb_sha1\"           >> $ver_file
                                    echo \"tikv_branch=$TIKV_BRANCH\"       >> $ver_file
                                    echo \"tikv_hash=$tikv_sha1\"           >> $ver_file
                                    echo \"pd_branch=$PD_BRANCH\"           >> $ver_file
                                    echo \"pd_hash=$pd_sha1\"               >> $ver_file
                                    echo \"tiflash_branch=$TIFLASH_BRANCH\" >> $ver_file
                                    echo \"tiflash_hash=$tiflash_sha1\"     >> $ver_file
                                    """
                                }
                            } else if (version == "stable") {
                                // Use the latest public release version in tiup mirror
                                sh """
                                v=\$(echo $branch | sed 's/release-\\(.*\\)/v\\1.x/g')
                                echo \"ver=\$v\"          > $ver_file
                                echo \"tidb_branch=\"    >> $ver_file
                                echo \"tidb_hash=\"      >> $ver_file
                                echo \"tikv_branch=\"    >> $ver_file
                                echo \"tikv_hash=\"      >> $ver_file
                                echo \"pd_branch=\"      >> $ver_file
                                echo \"pd_hash=\"        >> $ver_file
                                echo \"tiflash_branch=\" >> $ver_file
                                echo \"tiflash_hash=\"   >> $ver_file
                                """
                            }
                            sh """
                            cat $ver_file
                            """
                        }
                    }
                }

                stage("Test_" + branch + "_" + version + "_"  + testcase) {
                    container("docker-ops-ci") {
                        dir("/home/jenkins/agent/git/tiflash") {
                            // container("tiflash-docker") does not have python, run download binaries in this docker
                            def ver_file = "regression_test/download_ver.ti"
                            def binaries_dir = "/home/jenkins/agent/git/tiflash/binary/"
                            sh """
                            set -x
                            cat $ver_file
                            set +x
                            /home/jenkins/agent/git/tiflash/integrated/ops/ti.sh download /home/jenkins/agent/git/tiflash/regression_test/download.ti $binaries_dir
                            # Replace binaries using bin.paths
                            rm -f integrated/conf/bin.paths
                            cp regression_test/conf/bin.paths integrated/conf/
                            # show versions
                            /home/jenkins/agent/git/tiflash/integrated/ops/ti.sh /home/jenkins/agent/git/tiflash/regression_test/download.ti burn : up : ver : burn
                            """

                            def startTime = System.currentTimeMillis()
                            try {
                                timeout(maxRunTime) {
                                    sh "regression_test/schrodinger.sh " + testcase
                                }
                            } catch (err) {
                                def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(0, BigDecimal.ROUND_HALF_UP)
                                if (duration < Integer.parseInt(maxRunTime)) {
                                    throw err
                                }
                            }

                            sh "for f in \$(find . -name '*.log'); do echo \"LOG: \$f\"; tail -500 \$f; done"
                            sh "for f in \$(find /tmp/ti -name '*.log' | grep -v 'data' | grep -v 'tiflash/db'); do echo \"LOG: \$f\"; tail -500 \$f; done"

                            def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(0, BigDecimal.ROUND_HALF_UP)
                            if (duration < Integer.parseInt(maxRunTime)) {
                                currentBuild.result = "FAILURE"

                                def filename = "tiflash-jenkins-test-log-${env.JOB_NAME}-${env.BUILD_NUMBER}"
                                def filepath = "logs/pingcap/tiflash/${filename}.tar.gz"

                                sh """
                                  mkdir $filename
                                  for f in \$(find . -name '*.log'); do echo \"LOG: \$f\"; cp \$f ${filename}/; done
                                  for f in \$(find /tmp/ti -name '*.log' | grep -v 'data' | grep -v 'tiflash/db'); do echo \"LOG: \$f\"; cp \$f ${filename}/\${f//\\//_}; done
                                  ls -all "${filename}"
                                  tar zcf "${filename}.tar.gz" "${filename}"
                                  curl -F ${filepath}=@${filename}.tar.gz ${FILE_SERVER_URL}/upload
                                  echo "Download log file from http://fileserver.pingcap.net/download/${filepath}"
                                """
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
      def slackmsg = "TiFlash Schrodinger Test\n" +
              "Branch: `${branch}`\n" +
              "Version: `${version}`\n" +
              "Testcase: `${testcase}`\n" +
              "Result: `${currentBuild.result}`\n" +
              "Elapsed Time: `${duration}` Mins\n" +
              "https://internal.pingcap.net/idc-jenkins/blue/organizations/jenkins/tiflash_schrodinger_test/activity\n" +
              "https://internal.pingcap.net/idc-jenkins/job/tiflash_schrodinger_test/"
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
