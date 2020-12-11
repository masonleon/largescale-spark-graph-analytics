FROM cs6240/spark:latest as spark-livejournal

COPY core-site.xml /hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /hadoop/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml /hadoop/etc/hadoop/mapred-site.xml
COPY yarn-site.xml /hadoop/etc/hadoop/yarn-site.xml

COPY *.jar /


ADD local.sh /local.sh
RUN chmod a+x /local.sh

CMD ["/local.sh"]