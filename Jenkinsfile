pipeline {
    agent any

    environment {
        ANSIBLE_HOME = '/usr/bin/ansible'  // Path to Ansible binary, modify if necessary
    }

    stages {
        stage('Checkout Ansible Playbook') {
        steps {
            sshagent(['ray-github-ansible-key']) {
                git branch: 'main', 
                    url: 'git@github.com:RaymondNguyn/ansible-playbook.git', 
                    credentialsId: 'github-deploy-key-id'
            }
        }
    }

        stage('Run Ansible Playbook') {
            steps {
                sshagent(['ray-microservice-key-ansible']) {
                    sh '''
                        # Create a custom ansible.cfg file if needed
                        echo "[ssh_connection]" > ansible.cfg
                        echo "ssh_args = -o StrictHostKeyChecking=no" >> ansible.cfg
                        
                        # Run the playbook
                        ansible-playbook -i inventory.ini playbook.yaml
                    '''
                }
            }
        }
    }
}
