#!/bin/bash
#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
#
# *****************************************************************************
# Copyright (c) 2019 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Script to install Data Connector
#

INSTALLTYPE=$1
export INSTALLTYPE

# MAS Data Connector home
MASDCHOME=$HOME/ibm/masdc

#
# Check OS NAME
#
unames=$(uname -s)
case "${unames}" in
    Darwin*)    OSNAME=MacOS;;
    Linux*)     OSNAME=Linux;;
    *)          OSNAME="UNKNOWN:${unames}"
esac
echo "OS Name: ${OSNAME}"
echo "MASDCHOME: ${MASDCHOME}"


mkdir -p ${MASDCHOME}
mkdir -p ${MASDCHOME}/bin 
mkdir -p ${MASDCHOME}/lib

# 
# Install on localhost
#
if [ "${INSTALLTYPE}" == "localhost" ]; then
    cp *.py ${MASDCHOME}/bin/.
    cp *.sh ${MASDCHOME}/bin/.
    cp requirements.txt ${MASDCHOME}/bin/.
    cp ../lib/* ${MASDCHOME}/lib/.
fi
chmod +x ${MASDCHOME}/bin/*.sh

#
# Init log files
#
mkdir -p ${MASDCHOME}
mkdir -p ${MASDCHOME}/volume/logs
mkdir -p ${MASDCHOME}/volume/data/csv
mkdir -p ${MASDCHOME}/volume/config

#
# Install dependencies
# OpenJDK, Python and required Python pkgs
#
if [ ! -d ${MASDCHOME}/jre ]; then

    # Install openjdk
    mkdir -p ${MASDCHOME}/pkgs
    cd ${MASDCHOME}/pkgs
    if [ "${OSNAME}" == "Linux" ]; then
        echo "Download openjdk"
        curl https://download.java.net/java/GA/jdk13.0.1/cec27d702aa74d5a8630c65ae61e4305/9/GPL/openjdk-13.0.1_linux-x64_bin.tar.gz -L -o openjdk.tar.gz
        cd ${MASDCHOME}
        tar -xf ./pkgs/openjdk.tar.gz
        mv ${MASDCHOME}/jdk-13.0.1 ${MASDCHOME}/jre
    elif [ "${OSNAME}" == "MacOS" ]; then
        echo "Download openjdk"
        curl https://download.java.net/java/GA/jdk13.0.1/cec27d702aa74d5a8630c65ae61e4305/9/GPL/openjdk-13.0.1_osx-x64_bin.tar.gz -L -o openjdk.tar.gz
        cd ${MASDCHOME}
        tar -xf ./pkgs/openjdk.tar.gz
        mv ${MASDCHOME}/jdk-13.0.1.jdk/Contents/Home jre
        rm -rf ${MASDCHOME}/jdk-13.0.1.jdk
    fi
fi

echo


# Install Python
python3 --version
retval=$?
if [ $retval -eq 0 ]; then
    echo "Python is already installed"
else
    mkdir -p ${MASDCHOME}/pkgs
    cd ${MASDCHOME}/pkgs
    if [ "${OSNAME}" == "MacOS" ]; then
        echo "Download Python"
        curl https://www.python.org/ftp/python/3.8.1/python-3.8.1-macosx10.9.pkg -L -o python-3.8.1-macosx10.9.pkg
        echo "Install Python"
        installer -store -pkg ${MASDCHOME}/pkgs/python-3.8.1-macosx10.9.pkg -target /
    elif [ "${OSNAME}" == "Linux" ]; then
        echo "Install Python"
        apt-get update
        apt-get -y install python3-pip
        apt-get clean
    fi
fi

# Install pip3
pip3 --help > /dev/null 2>&1
retval=$?
if [ $retval -eq 0 ]; then
    echo "pip3 is already installed"
else
    if [ "${OSNAME}" == "MacOS" ]; then
        echo "Download get-pip.py"
        curl https://bootstrap.pypa.io/get-pip.py -L -o get-pip.py
        echo "Install pip"
        python3 get-pip.py
    elif [ "${OSNAME}" == "Linux" ]; then
        echo "Install Python"
        sudo apt-get update
        sudo apt-get -y install python3-pip
        sudo apt-get clean
    fi
fi

# Install required python packages
pip3 freeze | grep pandas > /dev/null 2>&1
retval=$?
if [ $retval -eq 0 ]; then
    echo "Required python modules are already installed"
else
    echo "Install required python modules"
    pip3 install --upgrade -r ${MASDCHOME}/bin/requirements.txt
fi

