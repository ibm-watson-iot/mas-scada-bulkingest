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

DI_HOME="$HOME/ibm/masdc"
export DI_HOME
DI_BIN="${DI_HOME}/bin"
export DI_BIN
DI_LIB="${DI_HOME}/lib"
export DI_LIB

PYTHON_HOME="${DI_HOME}/python-3.7.5"
export PYTHON_HOME

if [ -d ${PYTHON_HOME} ]
then
    PATH=$PATH:${DI_BIN}:${PYTHON_HOME}
    export PATH
    if [ "$1" == "alarm" ]
    then
        ${PYTHON_HOME}/python ${DI_BIN}/entity.py "$@"
    else
        ${PYTHON_HOME}/python ${DI_BIN}/entity.py "$@"
    fi
else
    PATH=$PATH:${DI_BIN}
    export PATH
    if [ "$1" == "alarm" ]
    then
        python3 ${DI_BIN}/entity.py "$@"
    else
        python3 ${DI_BIN}/entity.py "$@"
    fi
fi


