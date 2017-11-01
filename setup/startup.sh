#!/bin/bash

apachectl start
nohup mvn exec:java -X -f /usr/bin/Luzzu/luzzu-communications/pom.xml > /tmp/luzzu_output.log &
