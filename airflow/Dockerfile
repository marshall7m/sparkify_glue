# Dockerfile credit to Shubham Jain: https://github.com/jnshubham/aws-glue-local-etl-docker/blob/master/Dockerfile
FROM centos as glue

RUN yum install -y python3 java-1.8.0-openjdk java-1.8.0-openjdk-devel tar git wget zip gcc python3-devel && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

WORKDIR /glue
RUN git clone -b glue-1.0 https://github.com/awslabs/aws-glue-libs.git && \
    wget https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz && \
    wget https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz && \
    tar zxfv apache-maven-3.6.0-bin.tar.gz && \
    tar zxfv spark-2.4.3-bin-hadoop2.8.tgz && \
    mv $(rpm -q -l java-1.8.0-openjdk-devel | grep "/bin$" | rev | cut -d"/" -f2- |rev) /usr/lib/jvm/jdk
    
ENV SPARK_HOME /glue/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
ENV MAVEN_HOME /glue/apache-maven-3.6.0
ENV JAVA_HOME /usr/lib/jvm/jdk
ENV GLUE_HOME glue/aws-glue-libs
ENV PATH $PATH:$MAVEN_HOME/bin:$SPARK_HOME/bin:$JAVA_HOME/bin:$GLUE_HOME/bin
RUN sh aws-glue-libs/bin/glue-setup.sh && \
    #compile dependencies with maven build
    sed -i '/mvn -f/a rm /glue/aws-glue-libs/jarsv1/netty-*' /glue/aws-glue-libs/bin/glue-setup.sh && \
    sed -i '/mvn -f/a rm  /glue/aws-glue-libs/jarsv1/javax.servlet-3.*' /glue/aws-glue-libs/bin/glue-setup.sh && \
    #clean tmp dirs
    yum clean all

WORKDIR /../app

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install 'apache-airflow[postgres]'==1.10.10 \
    --constraint https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt
    
COPY glue_etl_scripts/ glue_etl_scripts/
COPY config/aws.cfg config/aws.cfg
ENV AIRFLOW_HOME /app/airflow
COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg
COPY scripts/entrypoint.sh scripts/entrypoint.sh
COPY scripts/connections.sh scripts/connections.sh

# ENTRYPOINT ["scripts/entrypoint.sh"]
# CMD ["webserver"]

CMD ["bash"]

