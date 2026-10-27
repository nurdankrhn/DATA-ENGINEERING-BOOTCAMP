FROM python:3.12

ENV SPARK_VERSION=4.1.3

# Install OpenJDK 21 (python:3.12 is Debian 13 / trixie: no openjdk-17 package anymore)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-21-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
RUN echo $JAVA_HOME

RUN apt-get update -y
RUN apt-get install -y libzbar-dev 
RUN apt-get install -y bash 
RUN apt-get install -y gcc 
RUN apt-get install -y git 
RUN apt-get install -y libc-dev 
RUN apt-get install -y curl wget vim nano   
RUN apt-get install -y iputils-ping telnet openssh-client net-tools
RUN apt-get install -y man 
RUN apt-get install -y unzip 
RUN apt-get install -y vim-tiny 
RUN apt-get install -y bc 
RUN apt-get install -y openssh-server 
RUN apt-get install -y thrift-compiler 
#RUN apt-get install -y netcat 
RUN apt-get install -y sudo
RUN apt-get install -y build-essential
RUN apt-get autoremove -y
# RUN apt-get install -y dos2unix
RUN apt-get clean

# install spark https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
RUN curl -fsSL --retry 5 --retry-all-errors -o spark-${SPARK_VERSION}-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# S3 (RustFS) and PostgreSQL jars.
# hadoop-aws must match the Hadoop inside Spark 4.1.3 (3.4.2). Hadoop 3.4 uses AWS SDK v2
# (software.amazon.awssdk:bundle, not the old aws-java-sdk-bundle-1.12.x) and also needs
# analyticsaccelerator-s3; without it s3a:// fails with ClassNotFoundException ...ObjectClient.
RUN cd /opt/spark/jars && \
    curl -fsSLO --retry 5 --retry-all-errors https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar && \
    curl -fsSLO --retry 5 --retry-all-errors https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar && \
    curl -fsSLO --retry 5 --retry-all-errors https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.2.1/analyticsaccelerator-s3-1.2.1.jar && \
    curl -fsSLO --retry 5 --retry-all-errors https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# SSH sessions don't get Docker ENV values. Without this, "ssh ssh_train@spark spark-submit ..."
# runs pip's spark-submit without SPARK_HOME and misses the jars above.
RUN printf '%s\n' "JAVA_HOME=${JAVA_HOME}" "SPARK_HOME=${SPARK_HOME}" "PATH=${PATH}" > /etc/environment
