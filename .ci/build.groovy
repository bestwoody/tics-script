def label = "build-tiflash"


catchError {

    def util = load('util.groovy')

    podTemplate(name: label, label: label, instanceCap: 5, containers: [
            containerTemplate(name: 'java', image: 'hub.pingcap.net/jenkins/centos7_golang-1.12_java', ttyEnabled: true, command: 'cat'),
    ]) {
        node(label) {
            stage("Checkout") {
                dir("tiflash") {
                    util.checkoutTiFlash("${params.ghprbActualCommit}", "${params.ghprbPullId}")
                }
                dir("tispark") {
                    git url: "https://github.com/pingcap/tispark.git", branch: "tiflash-ci-test"
                    container("java") {
                        sh """
                        archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tiflash/cache/tiflash-m2-cache_latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                        """
                        sh "mvn install -Dmaven.test.skip=true"
                    }
                }
            }
            dir("tiflash") {
                stage("Build") {
                    container("java") {
                        sh "cd computing/chspark && mvn install -DskipTests"
                    }
                }
                stage("Upload") {
                    echo "No need to upload currently."
                }
            }
        }
    }

}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def msg = "Build Result: `${currentBuild.currentResult}`" + "\n" +
            "Elapsed Time: `${duration} mins`" + "\n" +
            "${env.RUN_DISPLAY_URL}"

    echo "${msg}"

}
