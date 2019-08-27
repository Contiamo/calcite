pipeline {
    options {
        disableConcurrentBuilds()
    }
    agent {
        label 'master'
    }
    stages {
        stage('Git Checkout'){
            steps {
                checkout scm: [
	                $class: 'GitSCM',
	                branches: scm.branches,
	                doGenerateSubmoduleConfigurations: false,
	                extensions: [[$class: 'SubmoduleOption',
	                              disableSubmodules: false,
	                              parentCredentials: false,
	                              recursiveSubmodules: true,
	                              reference: '',
	                              trackingSubmodules: false]],
	                submoduleCfg: [],
	                userRemoteConfigs: scm.userRemoteConfigs
                ]
            }
        }
        stage('Maven Compile'){
        	steps {
        		sh 'mvn compile'
        	}
        }
        stage('Maven Tests'){
            steps {
                sh 'cd core && mvn test'
            }
        }
    }
}