#!/usr/bin/env bash

# Change this to your netid
netid=kxp210004

# Root directory of your project
PROJECT_DIR=/home/013/k/kx/kxp210004/MST

# Directory where the config file is located on your local system
CONFIG_LOCAL=$HOME/Desktop/DC-Minimum-Spanning-Tree/config.txt

CONFIG_DC=$PROJECT_DIR/config.txt

# Directory your java classes are in
BINARY_DIR=$PROJECT_DIR/target/mst.jar

# Your main project class
PROGRAM=Main

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    mapfile -t configFile 

    totalNodes=${configFile[0]}
    counter=1

    while [[ $counter -le $totalNodes ]]
    do
        line=(${configFile[$((counter))]})
        uid="${line[0]}"
        host="${line[1]}"
        port="${line[2]}"

        counter=$((counter + 1))

        osascript -e '
        tell app "Terminal"
            do script "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '$netid@$host' java -cp '$BINARY_DIR' '$PROGRAM' '$uid' '$CONFIG_DC'"
        end tell'
    done
)