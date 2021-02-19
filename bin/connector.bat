@ECHO OFF
REM
REM IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
REM
REM Copyright (c) 2019-2021 IBM Corporation and other Contributors.
REM
REM All rights reserved. This program and the accompanying materials
REM are made available under the terms of the Eclipse Public License v1.0
REM which accompanies this distribution, and is available at
REM http://www.eclipse.org/legal/epl-v10.html
REM (C) Copyright IBM Corp. 2019  All Rights Reserved.
REM

set DI_HOME="C:\IBM\masdc"
set DI_BIN=%DI_HOME%\bin
set DI_LIB=%DI_HOME%\lib

set CP=%DI_HOME%\jre\lib\*;%DI_HOME%\lib\*

%DI_HOME%\jre\bin\java -classpath "%CP%" com.ibm.wiotp.masdc.Connector "%1"

