FROM adoptopenjdk/openjdk11:alpine
VOLUME /tmp
ADD target/gcpavroprocessor-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]