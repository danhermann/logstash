LS_JAVA_OPTS="-Xmx1g -Xms1g" bin/logstash -b 128 -w 1 -f ingest.conf | pv | wc -c
