# IBM Watson IoT Bulk Data Ingest Connector for SCADA - Ignition

[![Build Status](https://travis-ci.com/ibm-watson-iot/mas-scada-bulkingest.svg?branch=master)](https://travis-ci.com/ibm-watson-iot/mas-scada-bulkingest)
[![GitHub issues](https://img.shields.io/github/issues/ibm-watson-iot/mas-scada-bulkingest.svg)](https://github.com/ibm-watson-iot/mas-scada-bulkingest/issues)
[![GitHub](https://img.shields.io/github/license/ibm-watson-iot/mas-scada-bulkingest.svg)](https://github.com/ibm-watson-iot/mas-scada-bulkingest/blob/master/LICENSE)

This project includes source to build an utility to:

* Extract data from SCADA historian
* Register device type, physical and logical interfaces in Watson IoT Platform service.
* Parse data to identify unique devices and register in Watson IoT Platform service.
* Transforms data and bulk upload data in Watson IoT Platform Data Lake.

You can build and install the data connector on an on-premise host system.
 
## Dependencies

* OpenJDK 11+
* Commons JCS
* org.json
* JDBC driver for MySQL and IBM DB2


## Supported Operating Environment

The connector is tested on the following operating system environment:

- Windows 10
- Windows 2016 server
- macOS Catalina
- Ubuntu 18.08


## Deploy Bulk Data Connector

Use the following steps to deploy the data connector.

To install on host operating system:
1. Get data connector source from GitHub. For details refer to the section ** Get data connector source **.
2. Run installation script included in the source. For details refer to the section ** Install on Host OS **.


## MAS data connector installation steps

### On Windows system:

You need Powershell on your Windows system. Powershell installation details: <br>
[How to install Powershell on Windows](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell-core-on-windows?view=powershell-7)?

Use powershell command to download install script from GitHub project, in a temprary directory.
```
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ibm-watson-iot/mas-scada-bulkingest/master/bin/install.ps1" -OutFile ".\install.ps1"
```

To configure connector tasks, lauch a Command Propmt with admin priviledges and run the following commands:
```
% powershell.exe -ExecutionPolicy Bypass .\configTask.ps1
```

### On macOS or Linux systems

Use one the following options to get the project source on your system:

1. Use a Web browser to download zip file of the GitHub project in /tmp directory. Open a shell prompt and run the follwing commands:
```
$ cd /tmp
$ unzip mas-scada-bulkingest-master.zip
```
2. Use curl command to download zip file of the GitHub project in /tmp directory
```
$ curl https://github.com/ibm-watson-iot/mas-scada-bulkingest/archive/master.zip -L -o /tmp/mas-scada-bulkingest-master.zip
$ cd /tmp
$ unzip mas-scada-bulkingest-master.zip
```
3. Use git command to clone the GitHub project
```
$ cd /tmp
$ git clone https://github.com/ibm-watson-iot/mas-scada-bulkingest
```

To install the connector, open a shell prompt, and run the install script:
```
$ cd /tmp/mas-scada-bulkingest
$ ./bin/install.sh
```

## Configure data connector

Refer to data connector documentation for data connector configuration:

https://ibm-watson-iot.github.io/mas-scada-bulkingest/





