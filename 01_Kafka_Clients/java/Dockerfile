FROM gradle:8.7-jdk17 AS build

WORKDIR /build
COPY java/ ./java/
COPY schemas/ ./schemas/

WORKDIR /build/java
RUN ./gradlew installDist -q

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app
COPY --from=build /build/java/build/install/kafka-order-clients/ ./
COPY java/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
