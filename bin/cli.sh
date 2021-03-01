#!/bin/bash
#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
#
# *****************************************************************************
# Copyright (c) 2021 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#

DI_HOME="$HOME/ibm/masdc"
export DI_HOME
DI_BIN="${DI_HOME}/bin"
export DI_BIN
DI_LIB="${DI_HOME}/lib"
export DI_LIB

CP="${DI_HOME}/jre/lib/*:${DI_HOME}/lib/*"
echo ${CP}

${DI_HOME}/jre/bin/java -classpath "${CP}" com.ibm.wiotp.masdc.CLIClient "$@"

