# Debezium Connect + Confluent Avro converter.
# The stock Debezium image ships without io.confluent.connect.avro.AvroConverter,
# so the converter plugin is fetched from Confluent Hub in a builder stage.
ARG DEBEZIUM_VERSION=3.7

FROM alpine:3.22 AS downloader
ARG AVRO_CONVERTER_VERSION=8.0.0
ARG JMX_EXPORTER_VERSION=1.0.1
RUN apk add --no-cache curl unzip
RUN curl -fsSL -o /tmp/avro-converter.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip" \
 && unzip -q /tmp/avro-converter.zip -d /opt/plugins \
 && mv /opt/plugins/confluentinc-kafka-connect-avro-converter-* /opt/plugins/avro-converter
# Prometheus JMX exporter javaagent: exposes Debezium/JVM MBeans as /metrics on :9404
RUN curl -fsSL -o /opt/jmx_prometheus_javaagent.jar \
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
COPY --from=downloader /opt/plugins/avro-converter/lib /kafka/connect/avro-converter
COPY --from=downloader /opt/jmx_prometheus_javaagent.jar /kafka/etc/jmx_prometheus_javaagent.jar
