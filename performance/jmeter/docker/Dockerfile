FROM eclipse-temurin:21-jre-alpine

ARG JMETER_VERSION=5.6.3

RUN apk add --no-cache bash curl tar

RUN curl -fsSL "https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz" \
  | tar -xz -C /opt \
  && ln -s "/opt/apache-jmeter-${JMETER_VERSION}" /opt/jmeter

ENV JMETER_HOME=/opt/jmeter
ENV PATH="${JMETER_HOME}/bin:${PATH}"

ENTRYPOINT ["/opt/jmeter/bin/jmeter"]
