ARG CASSANDRA_VERSION

FROM cassandra:${CASSANDRA_VERSION}

ENV JVM_EXTRA_OPTS="-Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.rmi.port=7199 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password -Djava.rmi.server.hostname=localhost"

ENV LOCAL_JMX='no'

ENV JMX_PORT='7199'

COPY ./jmxremote.password /etc/cassandra/jmxremote.password

RUN chmod 400 /etc/cassandra/jmxremote.password
