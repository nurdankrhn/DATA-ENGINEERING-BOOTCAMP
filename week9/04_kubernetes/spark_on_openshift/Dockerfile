FROM docker.io/library/spark:4.1.1-java21-python3

USER root
RUN mkdir -p /opt/spark/work-dir/app
COPY spark_on_k8s_app.py /opt/spark/work-dir/app/spark_on_k8s_app.py

# OpenShift starts containers with a random UID that is a member of group 0 (root group).
# Give group 0 the same rights as the owner, so that random UID can use work-dir.
RUN chgrp -R 0 /opt/spark/work-dir && chmod -R g=u /opt/spark/work-dir

USER spark
