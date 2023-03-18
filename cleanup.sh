#!/usr/bin/env bash


# Change this to your netid
netid=kxp210004


# Root directory of your project
PROJECT_DIR=$HOME/Desktop/DC-Minimum-Spanning-Tree

# Directory where the config file is located on your local system
CONFIG_LOCAL=$PROJECT_DIR/config.txt

n=0

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $i ]]
    do
    	read line
        host=$( echo $line | awk '{ print $2 }' )

        echo $host
        osascript -e '
        tell app "Terminal"
            do script "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '$netid@$host' killall -u '$netid'"
        end tell'
        sleep 1

        n=$(( n + 1 ))
    done
   
)


echo "Cleanup complete"