FROM eclipse-temurin:11-jre-alpine
WORKDIR /app
COPY target/min-mq-broker-1.0-SNAPSHOT.jar broker.jar
EXPOSE 8080 8081
ENTRYPOINT ["java", "-jar", "/app/broker.jar"]