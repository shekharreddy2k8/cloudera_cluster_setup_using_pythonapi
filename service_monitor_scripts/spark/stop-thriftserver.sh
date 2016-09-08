DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
if [ -e "$DIR_NAME/mas-spark-env.sh" ]; then
  source $DIR_NAME/mas-spark-env.sh
fi

if [ -z "$SPARK_HOME" ]; then
  echo "Spark not installed as expected"
  exit 1
fi

if [ ! -e $SPARK_HOME/sbin/stop-thriftserver.sh ]; then
  echo "Cannot stop spark-thriftserver because it is not properly installed"
  exit 1 
fi

#sudo -u hive $SPARK_HOME/sbin/stop-thriftserver.sh
$SPARK_HOME/sbin/stop-thriftserver.sh

