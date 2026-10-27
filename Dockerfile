FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY demo/pom.xml .
RUN mvn -B -ntp dependency:go-offline

COPY demo/src ./src
RUN mvn -B -ntp package dependency:copy-dependencies -DincludeScope=runtime

FROM eclipse-temurin:17-jre

WORKDIR /app
COPY --from=build /build/target/classes ./classes
COPY --from=build /build/target/dependency ./dependency

ENTRYPOINT ["java", "-cp", "/app/classes:/app/dependency/*", "flink.starter.Main"]
