FROM flink:1.12.3

RUN apt-get update ; apt-get install -y python3 python3-pip && pip3 install apache-flink && ln -s /usr/bin/python3.7 /usr/bin/python

ENTRYPOINT ["/docker-entrypoint.sh"]
