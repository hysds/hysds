#!/bin/sh
color='\e[1;32m'
nocolor='\e[0m'
clear
status=`sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v celery`
while [ 1 ]; do
  echo -e "$status" | column -t | sed -e "s:0:$(printf $color)0$(printf $nocolor):g"
  sleep 1
  status=`sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v celery`
  clear
done
