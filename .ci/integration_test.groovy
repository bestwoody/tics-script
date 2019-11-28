catchError {

    def util = load('util.groovy')

    def ticsTag = ({
        def m = params.ghprbCommentBody =~ /tics\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ticsTag ?: params.ghprbTargetBranch ?: 'raft'
    }).call()

    echo "ticsTag=${params.ticsTag} ghprbTargetBranch=${params.ghprbTargetBranch} ghprbCommentBody=${params.ghprbCommentBody}"

    def tasks = {}

    parallel(
        "OPS TI Test": {
            def label = "test-tiflash-ops-ti"

            podTemplate(name: label, label: label, instanceCap: 3, containers: [
                containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                        resourceRequestCpu: '2000m', resourceRequestMemory: '8Gi'),
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
                                try {
                                    sh "tests/ci/jenkins.sh"
                                } catch (err) {
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
            }
        },
        "Integration Test": {
            def label = "test-tiflash-integration"

            podTemplate(name: label, label: label, instanceCap: 3, containers: [
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
                                    sh "docker pull hub.pingcap.net/tiflash/tics:$ticsTag"
                                }

                                try {
                                    sh "TAG=$ticsTag sh -xe run.sh"
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
