#!/usr/bin/env bash

if [ -z "$MAS_SERVICE_MON_DIR" ]; then
  DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
  if [ -e "$DIR_NAME/../../conf/mas-env.sh" ]; then
    source $DIR_NAME/../../conf/mas-env.sh
  else
    export MAS_SERVICE_MON_DIR=`dirname $DIR_NAME`
  fi
fi

cmd=$1
shift

if [ "$#" == "0" ]; then
	echo "$0 [OPTIONS] <start|stop> <resourcemanager|nodemanager|jobhistoryserver> "
	exit 1
fi

service=$1
shift

while (( "$#" )); do
  cmd=$service
  service=$1
  shift
done

$MAS_SERVICE_MON_DIR/managenode.sh $cmd $service
