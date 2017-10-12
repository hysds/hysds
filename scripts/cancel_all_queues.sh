#!/bin/sh
if [[ $1 == "-h"  ||  $# > 2 ]]
then
    echo -e "Usage:\n\t$0 [block [skip]]" 1>&2
    echo -e "\t\tblock - (optional) blocks the program until queues are drained"  1>&2 
    echo -e "\t\tblock skip - (optional) skips queue consumption canceling, and just blocks" 1>&2
    exit 1
fi

#Cancels all queues
if [[ "$2" != "skip" ]]
then
    for queue in `sudo rabbitmqctl list_queues name | grep -v celery | grep -v 'Listing queues ...'`
    do
        echo "Stopping: ${queue}"
        celery control cancel_consumer ${queue} 
    done
fi
#Blocks the program on any non-finshed jobs
if [[ $1 == "block" ]]
then
    clear
    LOOP=`sudo rabbitmqctl list_queues name messages_unacknowledged | grep -v celery  | grep -v 'Listing queues ...' | grep -v '\s0$'`
    while [[ "$LOOP" != "" ]]
    do
        echo "Waiting on:"
        echo "${LOOP}" | column -t
        sleep 5
        LOOP=`sudo rabbitmqctl list_queues name messages_unacknowledged | grep -v celery  | grep -v 'Listing queues ...' | grep -v '\s0$'`
        clear
    done
fi
exit 0   
