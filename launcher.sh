#!/bin/bash

# Change this to your netid
netid=tsh160230

# Root directory of your project
PROJECT_DIR=/home/013/t/ts/tsh160230/DC-Minimum-Spanning-Tree

# Directory where the config file is located on your local system
CONFIG_LOCAL=/home/013/t/ts/tsh160230/DC-Minimum-Spanning-Tree/config.txt

# Directory your java classes are in
BINARY_DIR=$PROJECT_DIR/target/mst.jar

# Your main project class
PROGRAM=Main

n=0

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n < $i ]]
    do
    	read line
    	uid=$( echo $line | awk '{ print $1 }' )
      host=$( echo $line | awk '{ print $2 }' )
      port=$( echo $line | awk '{ print $3 }' )
	
	    gnome-terminal -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host java -cp $BINARY_DIR $PROGRAM $uid $CONFIG_LOCAL; exec bash" &

      n=$(( n + 1 ))
    done
)
