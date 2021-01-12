@Library('pipelinex@development') _

podTemplate(

    label: 'http-blaster-release',
    containers: [
        containerTemplate(name: 'jnlp', image: 'jenkins/jnlp-slave:4.0.1-1', workingDir: '/home/jenkins', resourceRequestCpu: '2000m', resourceLimitCpu: '2000m', resourceRequestMemory: '2048Mi', resourceLimitMemory: '2048Mi'),
        containerTemplate(name: 'golang', image: 'golang:1.14.12', workingDir: '/home/jenkins', ttyEnabled: true, command: 'cat'),
    ],
) {
      node("http-blaster-release") {
          common.notify_slack {
            container('golang') {

                stage('Obtain sources') {
                    checkout scm
                    sh "curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | BINDIR=/usr/local/bin/ sh"
                }

                stage('Build and release binaries') {

                    withCredentials([
                        usernamePassword(credentialsId: 'iguazio-prod-artifactory-credentials',
                            usernameVariable: 'ARTIFACTORY_IGUAZIO_USERNAME',
                            passwordVariable: 'ARTIFACTORY_IGUAZIO_SECRET'),
                        usernamePassword(credentialsId: 'iguazio-prod-artifactory-credentials',
                            usernameVariable: 'ARTIFACTORY_NAIPI_USERNAME',
                            passwordVariable: 'ARTIFACTORY_NAIPI_SECRET'),
                        string(credentialsId: 'iguazio-prod-git-user-token', variable: 'GITHUB_TOKEN')
                    ]) {
                        sh "/usr/local/bin/goreleaser release"
                    }
                }
            }
          }
      }
  }