#FROM maven:3.6.3-openjdk-8 AS hadoop
#
#ENV HADOOP_VERSION 2.9.1
#ENV SPARK_VERSION 2.3.1
#ENV SCALA_VERSION 2.11.12-4
#
##--------- Install Dependenncies ---------
#RUN apt-get update && \
#		apt-get install -y \
#			openssh-server \
#			pdsh \
#			build-essential \
#			net-tools
#
##--------- Generate SSH Keys ---------
#RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
#    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
#    chmod 0600 ~/.ssh/authorized_keys
#
#ADD ssh_config /root/.ssh/config
#
#RUN chmod 0600 ~/.ssh/config
#
## ---------- HADOOP -----------------------
#ENV HADOOP_URL "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
#
##RUN set -x && \
#RUN \
#    curl \
#      -fSL $HADOOP_URL \
#      -o /tmp/hadoop.tar.gz && \
#    tar \
#      -xvf /tmp/hadoop.tar.gz \
#      -C /usr/local/ && \
#    rm /tmp/hadoop.tar.gz*
#
#RUN ln -s /usr/local/hadoop-$HADOOP_VERSION hadoop
#
#ENV HADOOP_PREFIX /usr/local/hadoop-$HADOOP_VERSION
##ENV HADOOP_PREFIX /usr/local/hadoop
#ENV HADOOP_HOME $HADOOP_PREFIX
#ENV HADOOP_CONF_DIR=/etc/hadoop
#ENV HADOOP_INSTALL $HADOOP_HOME
#ENV HADOOP_MAPRED_HOME $HADOOP_HOME
#ENV HADOOP_COMMON_HOME $HADOOP_HOME
#ENV HADOOP_HDFS_HOME $HADOOP_HOME
#ENV YARN_HOME $HADOOP_HOME
#ENV HADOOP_YARN_HOME $HADOOP_HOME
#ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
#ENV HADOOP_CONF_DIR $YARN_CONF_DIR
#ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native
#ENV PATH $PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
#
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
#
#RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/local/openjdk-8:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
#
#RUN mkdir -p /root/tmpdata && \
#    mkdir -p /root/dfsdata/namenode && \
#    mkdir -p /root/dfsdata/datanode
#
## ADD core-site.xml $HADOOP_PREFIX/etc/hadoop/core-site.xml
## ADD hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
## ADD mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
## ADD yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
#
## RUN hdfs namenode -format
#
#CMD service ssh start && bash
#
## --------------- IDE specific Java 11 ------------
##RUN apt-get install -y \
##			default-jdk
#
#FROM hadoop as spark
#
## --------------- Scala ---------------------------
#RUN apt-get install -y \
#			scala=$SCALA_VERSION
#
#ENV SCALA_HOME /usr/share/scala-*
#ENV PATH $PATH:$SCALA_HOME/bin
#
## --------------- Spark ---------------------------
##ENV SPARK_URL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-without-hadoop.tgz"
##
###RUN set -x && \
##RUN \
##    curl \
##      -fSL $SPARK_URL \
##      -o /tmp/spark.tgz && \
##    tar \
##      -zxvf /tmp/spark.tgz \
##      -C /usr/local && \
##    rm /tmp/spark.tgz*
##
##RUN ln -s /usr/local/spark-2.3.1-bin-without-hadoop spark
##
##ENV SPARK_HOME /usr/local/spark
##ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
##
##RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ~/.bashrc
#
#RUN curl -s https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-without-hadoop.tgz | tar -xz -C /opt/\
#    && cd /opt && ln -s ./spark-2.3.1-bin-without-hadoop spark
#
#ENV SPARK_HOME /opt/spark
#ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
#ENV SPARK_DIST_CLASSPATH $HADOOP_HOME/bin/hadoop classpath)
#
#RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ~/.bashrc
#
#
#
#
#

FROM maven:3.6.3-openjdk-8 AS hadoop

#--------- Install SSH Server, PDSH, build-essential & net-tools ---------
RUN apt-get update\
    && apt-get install -y openssh-server pdsh build-essential net-tools less\
    && ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa\
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys\
    && chmod 0600 ~/.ssh/authorized_keys

ADD ssh_config /root/.ssh/config
RUN chmod 0600 ~/.ssh/config

# ---------- HADOOP 2.9.1 -----------------------
RUN curl -s https://archive.apache.org/dist/hadoop/common/hadoop-2.9.1/hadoop-2.9.1.tar.gz | tar -xz -C /usr/local/\
    && cd /usr/local && ln -s ./hadoop-2.9.1 hadoop

ENV HADOOP_PREFIX /usr/local/hadoop
ENV HADOOP_HOME $HADOOP_PREFIX
ENV HADOOP_INSTALL $HADOOP_HOME
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV YARN_HOME $HADOOP_HOME
ENV HADOOP_YARN_HOME $HADOOP_HOME
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HADOOP_CONF_DIR $YARN_CONF_DIR
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native
ENV PATH $PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/local/openjdk-8:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

RUN mkdir -p /root/tmpdata\
    && mkdir -p /root/dfsdata/namenode\
    && mkdir -p /root/dfsdata/datanode

# ADD core-site.xml $HADOOP_PREFIX/etc/hadoop/core-site.xml
# ADD hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
# ADD mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
# ADD yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# RUN hdfs namenode -format



CMD service ssh start && bash

# --------------- IDE specific Java 11 ------------
RUN apt-get install -y default-jdk




FROM hadoop as spark

# --------------- Scala ---------------------------
RUN apt-get install -y scala=2.11.12-4
ENV SCALA_HOME /usr/share/scala-2.11
ENV PATH $PATH:$SCALA_HOME/bin

# --------------- Spark ---------------------------
RUN curl -s https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-without-hadoop.tgz | tar -xz -C /opt/\
    && cd /opt && ln -s ./spark-2.3.1-bin-without-hadoop spark

ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"

RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ~/.bashrc
