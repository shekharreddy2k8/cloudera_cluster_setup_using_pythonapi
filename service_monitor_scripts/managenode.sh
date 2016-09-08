#!/usr/bin/env bash

usageMessage(){
  echo "Usage : $0 <status|start|stop|restart|installed> <service>"
  exit 2
}

if [ -z "$MAS_SERVICE_MON_DIR" ]; then
  DIR_NAME=`dirname $(readlink -f ${BASH_SOURCE[0]})`
  if [ -e "$DIR_NAME/../../conf/mas-env.sh" ]; then
    source $DIR_NAME/../../conf/mas-env.sh
  else
    export MAS_SERVICE_MON_DIR=`dirname $DIR_NAME`
  fi
fi

USER=`id -un`
if [ "$USER" == "hadoop" ] || [ "$USER" == "root" ]
then
        echo "$2 process triggered by $USER"
else
        echo "only root/hadoop can trigger the $2 process"
        exit -1
fi

todo=`echo $1 | tr '[:upper:]' '[:lower:]'`

`echo $todo |grep -w 'start\|stop\|status\|restart\|installed' >/dev/null`

status=$?

if [ $status -ne 0 ]; then
        usageMessage
fi

if [ "$2" == "" ]; then
        usageMessage
fi

checkInstalled()
{
  $MAS_SERVICE_MON_DIR/managenode.py installed $1
  return $?
}

checkStatus()
{
  $MAS_SERVICE_MON_DIR/managenode.py status $1
  return $?
}

stopService()
{
  $MAS_SERVICE_MON_DIR/managenode.py stop $1
  return $?
}

startService()
{
  $MAS_SERVICE_MON_DIR/managenode.py start $1
  return $?
}

restartService()
{
  $MAS_SERVICE_MON_DIR/managenode.py restart $1
  return $?
}

case "$todo" in
        'start')
                startService $2
                sleep 5
                checkStatus $2
        ;;

        'stop')
                stopService $2
        ;;

        'restart')
                echo "Restarting $2"
                restartService $2
                sleep 5
                checkStatus $2
        ;;

        'status')
                checkStatus $2
        ;;

        'installed')
                checkInstalled $2
        ;;
esac
