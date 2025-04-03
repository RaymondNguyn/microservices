def lint(String directory = '.') {
    sh "cd ${directory} && python -m pylint --fail-under=5 *.py"
}

def securityScan(String directory = '.') {
    // Using Bandit for Python security scanning (you can choose a different tool)
    sh "cd ${directory} && pip install bandit && bandit -r ."
}

def buildAndPush(String imageName, String contextPath, String tagPrefix, String buildNumber) {
    def version = "${tagPrefix}.${buildNumber}"
    sh "docker build -t rnguyen38/${imageName}:${version} ${contextPath}"
    sh "docker push rnguyen38/${imageName}:${version}"
}
