FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# /opt/ivy caches downloaded connector jars (mounted as a volume in compose)
RUN mkdir -p /opt/ivy /opt/app && chown -R spark:spark /opt/ivy /opt/app
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/app