catchError {

    def util = load('util.groovy')

    def ticsTag = ({
        def m = params.ghprbCommentBody =~ /tics\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ticsTag ?: params.ghprbTargetBranch ?: 'raft'
    }).call()

    def tidbBranch = ({
        def m = params.ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.tidbBranch ?: params.ghprbTargetBranch ?: 'master'
    }).call()

    def storageEngine = ({
        def m = params.ghprbCommentBody =~ /storage_engine\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        } else {
            return ""
        }
    }).call()

    echo "ticsTag=${ticsTag} tidbBranch=${tidbBranch} ghprbTargetBranch=${params.ghprbTargetBranch} ghprbCommentBody=${params.ghprbCommentBody}"

    def tasks = {}

    parallel(
        "OPS TI Test": {
            def label = "test-tiflash-ops-ti"

            podTemplate(name: label, label: label, instanceCap: 10, containers: [
                containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                        resourceRequestCpu: '10000m', resourceRequestMemory: '20Gi'),
                containerTemplate(name: 'docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                        envVars: [ envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375')],
                        alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
                containerTemplate(name: 'docker-ops-ci', image: 'hub.pingcap.net/tiflash/ops-ci:v11',
                        envVars: [ envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375') ],
                        alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            ]) {
                node(label) {
                    stage("Checkout") {
                        container("docker") {
                            sh "chown -R 1000:1000 ./"
                        }
                        dir("tiflash") {
                            util.checkoutTiFlash("${params.ghprbActualCommit}", "${params.ghprbPullId}")
                        }
                        dir("tispark") {
                            git url: "https://github.com/pingcap/tispark.git", branch: "tiflash-ci-test"
                            container("docker") {
                                sh """
                                archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tiflash/cache/tiflash-m2-cache_latest.tar.gz
                                if [ ! -d /root/.m2 ]; then curl -sL \$archive_url | tar -zx -C /root || true; fi
                                """
                                sh "mvn install -Dmaven.test.skip=true"
                            }
                        }
                    }
                    dir("tiflash/integrated") {
                        stage("OPS TI Test") {
                            container("docker-ops-ci") {
                              if(storageEngine != null && storageEngine != "") {
                                  sh "ls -all"

                                  sh "sed -i 's/storage_engine = \\\"dt\\\"/storage_engine = \\\"${storageEngine}\\\"/' conf/tiflash/config.toml"

                                  sh "cat conf/tiflash/config.toml"
                                }
                                try {
                                    timeout(120) {
                                        sh "tests/ci/jenkins.sh"
                                    }
                                } catch (err) {
                                    sh "for f in \$(find /tmp/ti -name '*.log' | grep -v 'data' | grep -v 'tiflash/db'); do echo \"LOG: \$f\"; tail -500 \$f; done"

                                    def filename = "tiflash-jenkins-test-log-${env.JOB_NAME}-${env.BUILD_NUMBER}"
                                    def filepath = "logs/pingcap/tiflash/${filename}.tar.gz"

                                    sh """
                                      mkdir $filename
                                      for f in \$(find /tmp/ti -name '*.log' | grep -v 'data' | grep -v 'tiflash/db'); do echo \"LOG: \$f\"; cp \$f ${filename}/\${f//\\//_}; done
                                      ls -all "${filename}"
                                      tar zcf "${filename}.tar.gz" "${filename}"
                                      curl -F ${filepath}=@${filename}.tar.gz ${FILE_SERVER_URL}/upload
                                      echo "Download log file from http://fileserver.pingcap.net/download/${filepath}"
                                    """

                                    throw err
                                }
                            }
                        }
                    }
                }
            }
        },
        "Integration Test": {
            def label = "test-tiflash-integration"

            podTemplate(name: label, label: label, instanceCap: 10, containers: [
                containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                        resourceRequestCpu: '2000m', resourceRequestMemory: '8Gi'),
                containerTemplate(name: 'docker', image: 'hub.pingcap.net/tiflash/docker:build-essential-java',
                        envVars: [ envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375')],
                        alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            ]) {
                node(label) {
                    stage("Checkout") {
                        container("docker") {
                            sh "chown -R 1000:1000 ./"
                        }
                        dir("tiflash") {
                            util.checkoutTiFlash("${params.ghprbActualCommit}", "${params.ghprbPullId}")
                        }
                        dir("tispark") {
                            git url: "https://github.com/pingcap/tispark.git", branch: "tiflash-ci-test"
                            container("docker") {
                                sh """
                                archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tiflash/cache/tiflash-m2-cache_latest.tar.gz
                                if [ ! -d /root/.m2 ]; then curl -sL \$archive_url | tar -zx -C /root || true; fi
                                """
                                sh "mvn install -Dmaven.test.skip=true"
                            }
                        }
                    }
                    dir("tiflash/tests/maven") {
                        stage("Test") {
                            container("docker") {
                                def firstTrial = true
                                retry(20) {
                                    if (firstTrial) {
                                        firstTrial = false
                                    } else {
                                        sleep time: 5, unit: "SECONDS"
                                    }
                                    sh "docker pull hub.pingcap.net/tiflash/tiflash:$ticsTag"
                                }

                                if(storageEngine != null && storageEngine != "") {
                                  sh "sed -i 's/\\\tstorage_engine = \\\"dt\\\"/\\\tstorage_engine = \\\"${storageEngine}\\\"/' config/tics.toml"
                                  sh "cat config/tics.toml"
                                }
                                try {
                                    sh "TAG=$ticsTag BRANCH=$tidbBranch sh -xe run.sh"
                                } catch(e) {
                                    sh "for f in \$(find log -name '*.log'); do echo \"LOG: \$f\"; tail -500 \$f; done"
                                    throw e
                                }
                            }
                        }
                    }
                }
            }
        },
    )

}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def msg = "Build Result: `${currentBuild.currentResult}`" + "\n" +
            "Elapsed Time: `${duration} mins`" + "\n" +
            "${env.RUN_DISPLAY_URL}"

    echo "${msg}"

}
