def checkoutTiFlash(commit, pullId) {
    checkout(changelog: false, poll: false, scm: [
            $class: "GitSCM",
            branches: [
                    [name: "${commit}"],
            ],
            userRemoteConfigs: [
                    [
                            url: "git@github.com:pingcap/tiflash.git",
                            refspec: "+refs/pull/${pullId}/*:refs/remotes/origin/pr/${pullId}/*",
                            credentialsId: "github-sre-bot-ssh",
                    ]
            ],
            extensions: [
                    [$class: 'PruneStaleBranch'],
                    [$class: 'CleanBeforeCheckout'],
            ],
    ])
}

return this
