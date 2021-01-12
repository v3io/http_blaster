@Library('pipelinex@development') _

podTemplate(
    label: 'http-blaster-lint',
    containers: [
        containerTemplate(name: 'jnlp', image: 'jenkins/jnlp-slave:4.0.1-1', workingDir: '/home/jenkins', resourceRequestCpu: '2000m', resourceLimitCpu: '2000m', resourceRequestMemory: '2048Mi', resourceLimitMemory: '2048Mi'),
        containerTemplate(name: 'golang', image: 'golang:1.14.12', workingDir: '/home/jenkins', ttyEnabled: true, command: 'cat'),
        containerTemplate(name: 'golangci-lint', image: 'golangci/golangci-lint:v1.32-alpine', workingDir: '/home/jenkins', ttyEnabled: true, command: 'cat'),
    ],
    envVars: [
        envVar(key: 'GO111MODULE', value: 'on'),
        envVar(key: 'GOPROXY', value: 'https://goproxy.devops.iguazeng.com')
    ],
) {
      node("http-blaster-lint") {
          common.notify_slack {
            stage('Run golangci-lint') {

                container('golang') {

                    checkout scm 
                    sh "go mod download"
                }

                container('golangci-lint') {

                    ansiColor('xterm') {
                        sh "golangci-lint run -v"
                    }
                }
            }
          }
      }
  }