FROM spark:3.5.3-java17
USER root
RUN mkdir -p /opt/spark/work-dir/app
COPY spark_on_k8s_app.py /opt/spark/work-dir/app/spark_on_k8s_app.py
USER spark
