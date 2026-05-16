FROM confluentinc/cp-kafka-connect:7.6.1

USER root

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.2 && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

# JDBC sink needs a MySQL driver on the plugin classpath.
RUN curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j-8.3.0.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar

USER appuser
