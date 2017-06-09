#!/bin/bash
PATH_INSTALLATION='/path/to' # the path where Luzzu and the Quality Metric repository should be installed
SET_WEBAPP=false # true if you want to access the web application from localhost; else false
WEBAPP_LOCATION='/path/to/local/www/html/' # localhost application location
INSTALL_EXAMPLES=false # true if you want to install the example metrics in luzzu

# Pull Luzzu and Quality Metrics from repository
echo "Installing Luzzu in $PATH_INSTALLATION"
cd $PATH_INSTALLATION
echo "Pulling Luzzu"
git clone https://github.com/EIS-Bonn/Luzzu.git

# install luzzu
cd $PATH_INSTALLATION/Luzzu/luzzu-communications/src/main/resources/
rm -rf log4j.xml
wget http://eis-bonn.github.io/Luzzu/setup/log4j.xml
cd $PATH_INSTALLATION/Luzzu
echo "Building Luzzu"
mvn clean install -Dmaven.test.skip=true;

cd $PATH_INSTALLATION
echo "Pulling Quality Metrics"
git clone https://github.com/diachron/quality.git

# install quality metrics
cd $PATH_INSTALLATION/quality
rm -rf pom.xml
wget http://eis-bonn.github.io/Luzzu/setup/pom.xml
echo "Building Quality Metrics"
mvn clean install -Dmaven.test.skip=true -Dfile.encoding=UTF-8

echo "Setting up Representational Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/representational
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-representation/target/*-dependencies.jar $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/representational/
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-representation/metrics.trig $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/representational/

echo "Setting up Intrinsic Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/intrinsic
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-intrinsic/target/*-dependencies.jar $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/intrinsic/
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-intrinsic/metrics.trig $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/intrinsic/

echo "Setting up Accessibility Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/accessibility
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-accessibility/target/*-dependencies.jar $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/accessibility/
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-accessibility/metrics.trig $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/accessibility/

echo "Setting up Contextual Metrics"
mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/contextual
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-contextual/target/*-dependencies.jar $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/contextual/
cp $PATH_INSTALLATION/quality/lod-qualitymetrics/lod-qualitymetrics-contextual/metrics.trig $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/contextual/

echo "Copying latest Data Quality Vocabularies"
cp $PATH_INSTALLATION/quality/quality-vocabulary/src/main/resources/vocabularies/dqm/dqm.ttl $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/
cp $PATH_INSTALLATION/quality/quality-vocabulary/src/main/resources/vocabularies/dqm/dqm-prob.ttl $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/
cp $PATH_INSTALLATION/quality/quality-vocabulary/src/main/resources/vocabularies/dqm/ebiqm.ttl $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/vocabs/

if [ $INSTALL_EXAMPLES ]; then
	echo "Setting up Example Metrics in Luzzu"
	mkdir $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/ebi
	cp $PATH_INSTALLATION/quality/ebi/target/*-dependencies.jar $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/ebi/
	cp $PATH_INSTALLATION/quality/ebi/metrics.trig $PATH_INSTALLATION/Luzzu/luzzu-communications/externals/metrics/ebi/
	echo "Examples can be found: PATH_INSTALLATION/quality/examples/"	
fi

if [ $SET_WEBAPP ]; then
	echo "Copying Luzzu Web App to $WEBAPP_LOCATION"
	cp -r $PATH_INSTALLATION/Luzzu/luzzu-webapp/site/* $WEBAPP_LOCATION
	echo "ServerName localhost" << /etc/apache2/apache2.conf
fi

cd $PATH_INSTALLATION
