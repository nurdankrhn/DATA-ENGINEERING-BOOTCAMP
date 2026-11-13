
FROM spark:3.5.3-java17

USER root

# Create the application directory
RUN mkdir -p /opt/spark/work-dir/app

# Copy the PySpark homework application
COPY --chown=spark:spark spark_on_k8s_hw.py /opt/spark/work-dir/app/spark_on_k8s_hw.py

USER spark
