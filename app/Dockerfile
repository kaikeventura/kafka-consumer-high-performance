FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /app

COPY pom.xml .

RUN mvn dependency:go-offline -B

COPY src ./src

RUN mvn package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", \
    "-XX:+UseG1GC", \
    "-XX:MaxGCPauseMillis=20", \
    "-XX:G1HeapRegionSize=4m", \
    "-XX:+ParallelRefProcEnabled", \
    "-XX:ActiveProcessorCount=2", \
    "-Xms512m", \
    "-Xmx900m", \
    "-jar", "app.jar"]
