#######################
# Spark configuration #
#######################
CLOUDERA_HOME=/opt/cloudera/parcels/CDH
SPARK_HOME=""
if [ -d $CLOUDERA_HOME ]; then
  CLOUDERA_HOME="$(readlink -f "$CLOUDERA_HOME")"
  echo $CLOUDERA_HOME
  SPARK_HOME=$CLOUDERA_HOME/lib/spark
else 
  hdp_spark=`find /usr/hdp/ -name start-thriftserver.sh`
  if [ ! -z "$hdp_spark" ]; then
    hdp_spark=`dirname \`dirname $hdp_spark\``
    SPARK_HOME="$hdp_spark"
  fi
fi

if [ -z "$SPARK_HOME" ]; then
  echo "Spark not installed as expected"
  exit 1
fi

export SPARK_HOME=$SPARK_HOME

####################################
# Spark Thriftserver Configuration #
####################################
export SPARK_THRIFT_SERVER_PORT=20000
export SPARK_MIN_EXEC=0
export SPARK_MAX_EXEC=16
export SPARK_EXEC_MEM=1g
export SPARK_DRIVER_MEM=2g




