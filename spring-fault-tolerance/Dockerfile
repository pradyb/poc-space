# syntax=docker/dockerfile:experimental
FROM gradle:7.4.1-jdk11-alpine AS build
WORKDIR /app

COPY . /app
RUN --mount=type=cache,target=/root/.gradle ./gradlew clean build -x test -x check
RUN mkdir -p build/dependency && (cd build/dependency; jar -xf ../libs/app-*.jar)

FROM openjdk:11.0.7-jre-slim
VOLUME /tmp
ARG DEPENDENCY=/app/build/dependency
COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app
ENTRYPOINT ["java","-cp","/app:/app/lib/*","com.prady.sample.SpringFaultToleranceApplication"]