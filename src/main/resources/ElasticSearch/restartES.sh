#!/bin/sh

export PATH=$JAVA_HOME/bin:$PATH

ps -ef | grep elasticsearch | grep -v grep | awk '{print "kill -9 "$2}'|sh

/home/hadoop/app/elasticsearch-6.1.2/bin/elasticsearch -d
