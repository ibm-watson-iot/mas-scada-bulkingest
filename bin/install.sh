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
mkdir -p ${MASDCHOME}/volume/data
mkdir -p ${MASDCHOME}/volume/config

#
# Install dependencies
# OpenJDK
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

