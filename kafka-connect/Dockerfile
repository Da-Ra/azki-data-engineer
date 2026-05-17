FROM confluentinc/cp-kafka-connect:8.1.3

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4

RUN curl -fSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j-8.3.0.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar
