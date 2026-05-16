FROM confluentinc/cp-kafka:7.6.1
USER root
RUN mkdir -p /usr/share/jmx-exporter && \
    curl -fsSL -o /usr/share/jmx-exporter/jmx_prometheus_javaagent-1.1.0.jar \
      https://raw.githubusercontent.com/confluentinc/jmx-monitoring-stacks/main/shared-assets/jmx-exporter/jmx_prometheus_javaagent-1.1.0.jar
COPY monitoring/jmx-exporter/kafka_client.yml /usr/share/jmx-exporter/kafka_client.yml
USER appuser
