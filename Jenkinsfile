pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Code was checked out from Git.'
                sh 'git branch || true'
                sh 'git log -1 --oneline || true'
            }
        }

        stage('Explore Workspace') {
            steps {
                echo 'Listing repository files...'
                sh 'pwd'
                sh 'ls -la'
            }
        }

        stage('Build') {
            steps {
                echo 'Simulating a build...'
                sh '''
                    mkdir -p build
                    echo "Hello Jenkins" > build/result.txt
                    date >> build/result.txt
                '''
            }
        }

        stage('Test') {
            steps {
                echo 'Running simple test...'
                sh 'grep "Hello Jenkins" build/result.txt'
            }
        }

        stage('Archive') {
            steps {
                archiveArtifacts artifacts: 'build/result.txt', fingerprint: true
            }
        }
    }

    post {
        always {
            echo 'Pipeline finished.'
        }
    }
}