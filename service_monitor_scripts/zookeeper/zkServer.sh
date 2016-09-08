#!/usr/bin/env bash

if [ -z "$MAS_SERVICE_MON_DIR" ]; then
  DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
  if [ -e "$DIR_NAME/../../conf/mas-env.sh" ]; then
    source $DIR_NAME/../../conf/mas-env.sh
  else
    export MAS_SERVICE_MON_DIR=`dirname $DIR_NAME`
  fi
fi

$MAS_SERVICE_MON_DIR/managenode.sh $1 ZOOKEEPER
