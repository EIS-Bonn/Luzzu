#!/bin/bash

apachectl start
mvn exec:java -X -f /usr/bin/Luzzu/luzzu-communications/pom.xml
