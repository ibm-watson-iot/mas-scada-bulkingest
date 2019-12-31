@ECHO OFF
REM
REM IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
REM
REM Copyright (c) 2019 IBM Corporation and other Contributors.
REM
REM All rights reserved. This program and the accompanying materials
REM are made available under the terms of the Eclipse Public License v1.0
REM which accompanies this distribution, and is available at
REM http://www.eclipse.org/legal/epl-v10.html
REM (C) Copyright IBM Corp. 2019  All Rights Reserved.
REM

set DI_HOME=%HOMEPATH%\ibm\masdc
set DI_BIN=%DI_HOME%\bin
set DI_LIB=%DI_HOME%\lib

set LIFT_HOME=%DI_HOME%\lift-cli
set LIFT_CMD=%LIFT_HOME%\bin\lift.bat
set LIFT_JAVA_HOME=%LIFT_HOME%\jre
set LIFT_DATA=%LIFT_HOME%\data
set LIFT_SEGMENT_SOURCE=PRODUCTION

set PYTHON_HOME=%DI_HOME%\python-3.7.5

set PATH=%PATH%;%DI_BIN%;%PYTHON_HOME%

%PYTHON_HOME%\python.exe %DI_BIN%\run.py %*


