@echo off

REM IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
REM *****************************************************************************
REM Copyright (c) 2019 IBM Corporation and other Contributors.
REM All rights reserved. This program and the accompanying materials
REM are made available under the terms of the Eclipse Public License v1.0
REM which accompanies this distribution, and is available at
REM http://www.eclipse.org/legal/epl-v10.html
REM *****************************************************************************
REM Script to install Data Connector

REM Invoke Powershell script to download packages and install

powershell.exe -ExecutionPolicy Bypass .\bin\install.ps1

