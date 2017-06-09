#!/bin/bash

##################
#  Dependencies  #
##################

sudo apt-get update
echo "Installing Java"
sudo apt-get -y install openjdk-8-jdk

echo "Installing Maven"
sudo apt-get -y install maven

echo "Installing Python Tools"
sudo apt-get -y install python-setuptools

echo "Installing GIT"
sudo apt-get -y install git

echo "Installing Zip"
sudo apt-get -y install zip

echo "Installing Apache"
sudo apt-get -y install apache2


####################
#  Python Scripts  #
####################

cd /tmp
sudo easy_install rdflib
sudo easy_install http://cheeseshop.python.org/packages/source/p/pyparsing/pyparsing-1.5.7.tar.gz
sudo easy_install unirest
git clone https://github.com/RDFLib/rdflib-jsonld.git
cd rdflib-jsonld
sudo python setup.py build
sudo python setup.py install


