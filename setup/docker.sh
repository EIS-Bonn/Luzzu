#!/bin/bash
PATH_INSTALLATION='/usr/bin' # the path where Luzzu and the Quality Metric repository should be installed
SET_WEBAPP=true # true if you want to access the web application from localhost; else false
WEBAPP_LOCATION='/var/www/html' # localhost application location
INSTALL_EXAMPLES=true # true if you want to install the example metrics in luzzu

# Pull Luzzu and Quality Metrics from repository
mkdir /Luzzu
echo "Installing Luzzu in $PATH_INSTALLATION"
cd $PATH_INSTALLATION
echo "Pulling Luzzu"
git clone https://github.com/EIS-Bonn/Luzzu.git -b Luzzu3

# install luzzu
cd $PATH_INSTALLATION/Luzzu/luzzu-communications/src/main/resources/
rm -rf log4j.xml
wget http://eis-bonn.github.io/Luzzu/setup/log4j.xml
cd $PATH_INSTALLATION/Luzzu
echo "Building Luzzu"
mvn clean install -Dmaven.test.skip=true;

cd $PATH_INSTALLATION
echo "Downloading Quality Metrics"
mkdir $PATH_INSTALLATION/tmp
cd $PATH_INSTALLATION/tmp
wget http://s001.adaptcentre.ie/LDQM/accessibility.zip
wget http://s001.adaptcentre.ie/LDQM/contextual.zip
wget http://s001.adaptcentre.ie/LDQM/intrinsic.zip
wget http://s001.adaptcentre.ie/LDQM/representational.zip


echo "Setting up Representational Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/representational
unzip representational.zip
cp representational/metrics/representational/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/representational/
cp representational/vocabs/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/


echo "Setting up Intrinsic Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/intrinsic
unzip intrinsic.zip
cp intrinsic/metrics/intrinsic/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/intrinsic/
cp intrinsic/vocabs/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/

echo "Setting up Accessibility Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/accessibility
unzip accessibility.zip
cp accessibility/metrics/accessibility/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/accessibility/
cp accessibility/vocabs/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/

echo "Setting up Contextual Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/contextual
unzip contextual.zip
cp contextual/metrics/contextual/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/contextual/
cp contextual/vocabs/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/



if [ $INSTALL_EXAMPLES ]; then
	echo "Setting up Example Metrics in Luzzu"
	wget https://www.dropbox.com/s/lqe4xr1h55nxkas/ebi.zip
	unzip ebi.zip
	mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/ebi
	cp ebi/metrics/ebi/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/ebi/
	cp ebi/vocabs/* $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/
	mkdir $PATH_INSTALLATION/examples
	cp ebi/examples/* $PATH_INSTALLATION/examples
fi

cd $PATH_INSTALLATION
rm -rf $PATH_INSTALLATION/tmp

if [ $SET_WEBAPP ]; then
	echo "Copying Luzzu Web App to $WEBAPP_LOCATION"
	cp -r $PATH_INSTALLATION/Luzzu/luzzu-webapp/site/* $WEBAPP_LOCATION
	echo "ServerName localhost" >> /etc/apache2/apache2.conf
fi

cd $PATH_INSTALLATION
