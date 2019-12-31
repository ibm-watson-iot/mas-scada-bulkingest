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

# Set the following variables
# MAS Data Connector home
MASDCHOME=$HOME/ibm/masdc
# Install Lift CLI
INSTALLLIFT=no

# 
# No Need to edit beyod this line
#

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


mkdir -p ${MASDCHOME}

# 
# Install Packages
#
mkdir -p ${MASDCHOME}/bin 
mkdir -p ${MASDCHOME}/lib
cp *.py ${MASDCHOME}/bin/.
cp *.sh ${MASDCHOME}/bin/.
cp *.bat ${MASDCHOME}/bin/.
cp ../lib/* ${MASDCHOME}/lib/.
chmod +x ${MASDCHOME}/bin/*.sh

#
# Init log files
#
mkdir -p ${MASDCHOME}
mkdir -p ${MASDCHOME}/volume/logs
mkdir -p ${MASDCHOME}/volume/data/csv
mkdir -p ${MASDCHOME}/volume/config


