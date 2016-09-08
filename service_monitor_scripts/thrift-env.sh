CLOUDERA_HOME=/opt/cloudera/parcels/CDH
SPARK_HOME=$CLOUDERA_HOME/lib/spark
export SPARK_HOME=$SPARK_HOME
####################################
# Spark Thriftserver Configuration #
####################################
export SPARK_THRIFT_SERVER_PORT=20000
export SPARK_MIN_EXEC=0
export SPARK_MAX_EXEC=16
export SPARK_EXEC_MEM=1g
export SPARK_DRIVER_MEM=2g

export CLASS="org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.9-src.zip:${PYTHONPATH}"

