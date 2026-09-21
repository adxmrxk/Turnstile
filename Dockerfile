# Build stage: compile and package with the same Maven and JDK the project targets.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
# Resolve dependencies first so a source-only change reuses this layer.
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

# Runtime stage: just a JRE, running as an unprivileged user.
FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 --no-create-home turnstile
WORKDIR /app
COPY --from=build /build/target/turnstile-*-SNAPSHOT.jar /app/turnstile.jar
USER turnstile
EXPOSE 8080
# The prod profile refuses to start without its secrets, so a misconfigured
# container fails immediately instead of running half set up.
ENV SPRING_PROFILES_ACTIVE=prod
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/turnstile.jar"]
