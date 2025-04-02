pipeline {
    agent any

    environment {
        ANSIBLE_HOME = '/usr/bin/ansible'  // Path to Ansible binary, modify if necessary
    }

    stages {
        stage('Checkout Ansible Playbook') {
            steps {
                git branch: 'main', url: 'git@github.com:RaymondNguyn/ansible-playbook.git'
            }
        }

        stage('Run Ansible Playbook') {
            steps {
                ansiblePlaybook(
                    playbook: 'playbook.yaml',
                    inventory: 'inventory.ini',
                )
            }
        }
    }
}
