###################################################
# Set environmental variables required to access  #
# MAS service install and management scripts.     #
###################################################

DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
export MAS_ROOT_DIR=`dirname $DIR_NAME`
export MAS_SERVICE_MON_DIR=$MAS_ROOT_DIR/service_monitor_scripts
export HADOOP_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/hadoop
export HADOOP_MR_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/hadoop
export HIVE_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/hive
export HBASE_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/hbase
export SPARK_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/spark
export YARN_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/yarn
export ZOOKEEPER_SCRIPT_DIR=$MAS_SERVICE_MON_DIR/zookeeper

