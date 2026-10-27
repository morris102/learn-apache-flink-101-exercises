pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                sh 'ls -la'
                git branch: 'main', credentialsId: 'git-creds', url: 'https://github.com/morris102/learn-apache-flink-101-exercises.git'
            }
        }
        stage('Maven Package') {
            steps {
                script {
                    def mvnHome = tool name: 'Maven-3.9', type: 'maven'
                    def mvnCommand = "${mvnHome}/bin/mvn"
                    dir('demo') {
                        sh "${mvnCommand} clean package -DskipTests"
                    }
                }
            }
        }
        stage('Build Docker Image') {
            steps {
                sh 'docker build -t stevenguyen9x/flink-exercises:1.0.0 .'
            }
        }

        stage('Push Docker Image') {
            steps {
                withCredentials([string(credentialsId: 'docker-pwd', variable: 'dockerPwd')]) {
                    sh 'docker login -u stevenguyen9x -p $dockerPwd'
                    sh 'docker push stevenguyen9x/flink-exercises:1.0.0'
                }
            }
        }
    }
}
