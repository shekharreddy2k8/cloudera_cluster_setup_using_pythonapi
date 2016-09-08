DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
if [ -e "$DIR_NAME/mas-spark-env.sh" ]; then
  source $DIR_NAME/mas-spark-env.sh
fi

if [ -z "$SPARK_HOME" ]; then
  echo "Spark not installed as expected"
  exit 1
fi

if [ ! -e $SPARK_HOME/sbin/start-thriftserver.sh ]; then
  echo "Cannot start spark-thriftserver because it is not properly installed"
  exit 1 
fi

#sudo -u hive
$SPARK_HOME/sbin/start-thriftserver.sh --master yarn-client --executor-memory $SPARK_EXEC_MEM --driver-memory $SPARK_DRIVER_MEM --conf spark.dynamicAllocation.minExecutors=$SPARK_MIN_EXEC --conf spark.dynamicAllocation.maxExecutors=$SPARK_MAX_EXEC --hiveconf hive.server2.thrift.port=$SPARK_THRIFT_SERVER_PORT

