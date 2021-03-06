FROM maven:3.5-alpine

COPY ngdbc.jar pipeline/

COPY pom.xml pipeline/

COPY Jenkinsfile pipeline/

COPY src/ pipeline/src/

COPY dev/ pipeline/dev/

COPY config pipeline/config

COPY hana-spark-connector.iml pipeline/ 

WORKDIR pipeline/

RUN  mvn install:install-file -Dfile=/pipeline/ngdbc.jar -DgroupId=com.sap.db -DartifactId=ngdbc -Dversion=2.1.2 -Dpackaging=jar

RUN ["mvn", "install", "-Dmaven.test.skip=true"]

ENTRYPOINT [ "/bin/bash" ]
