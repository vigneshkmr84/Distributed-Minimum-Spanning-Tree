#!/bin/bash

cd ~/
wget https://dlcdn.apache.org/maven/maven-3/3.9.1/binaries/apache-maven-3.9.1-bin.zip 
unzip apache-maven-3.9.1-bin.zip 
echo 'export PATH="${HOME}/apache-maven-3.9.1/bin:${PATH}"' >> ~/.bashrc
exec bash