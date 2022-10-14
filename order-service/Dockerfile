# Builder ############################################
FROM maven:3.8.6-openjdk-11-slim AS builder

ARG BUILD_HOME=/opt/app
WORKDIR $BUILD_HOME

ARG MODULE=order-service
ARG PROMETHEUS_AGENT_VERSION=0.17.0

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${PROMETHEUS_AGENT_VERSION}/jmx_prometheus_javaagent-${PROMETHEUS_AGENT_VERSION}.jar -O jmx_prometheus_javaagent.jar

COPY pom.xml .
COPY bom/pom.xml bom/pom.xml
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY domain-model/pom.xml domain-model/pom.xml
COPY domain-model/src domain-model/src
COPY order-service/pom.xml order-service/pom.xml
COPY order-service/src order-service/src
COPY order-service/config order-service/config
COPY payment-service/pom.xml payment-service/pom.xml
COPY payment-service/src payment-service/src
COPY payment-service/config payment-service/config
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY inventory-service/src inventory-service/src
COPY inventory-service/config inventory-service/config

RUN --mount=type=cache,target=/root/.m2 mvn -B -e -pl ${MODULE} -am clean install -DskipTests=true

# App ############################################
FROM adoptopenjdk/openjdk11:ubi

ARG BUILD_HOME=/opt/app
WORKDIR $BUILD_HOME

ARG MODULE=order-service

COPY --from=builder $BUILD_HOME/jmx_prometheus_javaagent.jar jmx_prometheus_javaagent.jar
COPY --from=builder $BUILD_HOME/$MODULE/target/${MODULE}-jar-with-dependencies.jar app.jar
COPY --from=builder $BUILD_HOME/$MODULE/config/application-docker.properties application.properties

EXPOSE 4545
EXPOSE 5000

ENV JAVA_TOOL_OPTIONS "-Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.port=5000 \
  -Dcom.sun.management.jmxremote.rmi.port=5000 \
  -Dcom.sun.management.jmxremote.host=0.0.0.0 \
  -Djava.rmi.server.hostname=0.0.0.0 \
  -javaagent:jmx_prometheus_javaagent.jar=1234:/usr/share/jmx-exporter/kafka_client.yml"

CMD ["java", "-jar", "app.jar","application.properties"]
