```commandline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.1 \
--repositories https://repo1.maven.org/maven2/ /opt/bitnami/spark/spark-apps/streaming.py
```