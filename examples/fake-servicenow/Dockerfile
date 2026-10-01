# Fake ServiceNow for the docker-compose "fake" profile.
#
# Expects lib/ to be populated first by ./build.sh (snow-core jar, its test-jar and the
# test-scope classpath: WireMock, Jackson, logback, connect-api). Nothing is built here so
# the image needs no Maven and no source tree.
FROM eclipse-temurin:17-jre

WORKDIR /app
COPY lib/ /app/lib/

EXPOSE 8090
# The JVM expands the classpath wildcard itself, so no shell is needed.
ENTRYPOINT ["java", "-cp", "/app/lib/*", "sh.oso.servicenow.testing.FakeServiceNowMain"]
CMD ["--port", "8090"]
