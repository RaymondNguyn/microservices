def lint(String directory = '.') {
    sh "cd ${directory} && python -m pylint --fail-under=5 *.py"
}

def securityScan(String directory = '.') {
    // Using Bandit for Python security scanning (you can choose a different tool)
    sh "cd ${directory} && pip install bandit && bandit -r ."
}

def buildAndPush(String imageName,String contextPath, String version = 'latest') {
    sh "docker build -t rnguyen38/${imageName}:${version} ${contextPath}"
    sh "docker push rnguyen38/${imageName}:${version}"
}

def deploy(String serviceNames) {
    // Using SSH to connect to your 3855 VM and redeploy
    sshagent(['your-ssh-credentials-id']) {
        sh "root@64.23.244.25 'cd /path/to/your/project && docker-compose pull ${serviceNames} && docker-compose up -d ${serviceNames}'"
    }
}