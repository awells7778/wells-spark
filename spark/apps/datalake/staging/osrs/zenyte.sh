/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar \
--driver-memory 1G \
--executor-memory 1G \
--packages "org.apache.hadoop:hadoop-aws:3.3.0,com.amazonaws:aws-java-sdk-bundle:1.11.900" \
/opt/spark-apps/datalake/osrs/zenyte.py