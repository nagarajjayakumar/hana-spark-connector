FROM yogeshprabhu/spark2.3

COPY ngdbc.jar pipeline/

COPY pom.xml pipeline/

COPY Jenkinsfile pipeline/

COPY src/ pipeline/src/

COPY dev/ pipeline/dev/

COPY config pipeline/config

COPY hana-spark-connector.iml pipeline/ 

WORKDIR pipeline/

RUN  mvn install:install-file -Dfile=/opt/spark/work-dir/pipeline/ngdbc.jar -DgroupId=com.sap.db -DartifactId=ngdbc -Dversion=2.1.2 -Dpackaging=jar

RUN ["mvn", "install", "-Dmaven.test.skip=true"]

ENTRYPOINT [ "/bin/bash" ]
