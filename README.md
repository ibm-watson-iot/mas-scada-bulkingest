# SCADA Bulk Data Ingest Connector for IBM Watson IoT

[![Build Status](https://travis-ci.com/ibm-watson-iot/mas-scada-bulkingest.svg?branch=master)](https://travis-ci.com/ibm-watson-iot/mas-scada-bulkingest)
[![GitHub issues](https://img.shields.io/github/issues/ibm-watson-iot/mas-scada-bulkingest.svg)](https://github.com/ibm-watson-iot/mas-scada-bulkingest/issues)
[![GitHub](https://img.shields.io/github/license/ibm-watson-iot/mas-scada-bulkingest.svg)](https://github.com/ibm-watson-iot/mas-scada-bulkingest/blob/master/LICENSE)

This project includes source to build an utility to:

* Extract data from SCADA historian in CSV format.
* Register device type, physical and logical interfaces in Watson IoT Platform service.
* Parse data to identify unique devices and register in Watson IoT Platform service.
* Optionally send extracted data using MQTT protocol.
* Transforms data based on user defined rules and bulk upload data in Watson IoT Platform Data Lake.

You can build and install the data connector on an on-premise host system.
 
A Docker image can be build using provided Dockerfile that defines an image that is based on Ubuntu.
You can build and run container on any operating environment that has docker-ce
(Docker Community Edition) environment set.  For information on Docker Community
Edition for your operating environment, see [About Docker CE](https://docs.docker.com/install/). 


## Dependencies

* Python 3.X (https://www.anaconda.com/distribution/)

* OpenJDK 13.0.1
* Docker (for dockerized version)


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

To build and run data connector docker container:
1. Get data connector source from GitHub. For details refer to the section ** Get data connector source **.
2. Build and run connector. For details refer to the section ** Build and run docker image **.



## Get data connector source

### On macOS or Linux systems:

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

### On Windows system:

Use one the following options to get the project source on your system:

1. Use a Web browser to download zip file of the GitHub project in c:\temp directory. Lauch a Command Propmt and run the following commands:
```
% cd c:\temp
% unzip mas-scada-bulkingest-master.zip
```
2. Use curl command to download zip file of the GitHub project in c:\temp directory. Lauch a Command Propmt and run the following commands:
```
% curl https://github.com/ibm-watson-iot/mas-scada-bulkingest/archive/master.zip -L -o c:\temp\mas-scada-bulkingest-master.zip
% cd c:\temp
% unzip mas-scada-bulkingest-master.zip
```
3. Use git command to clone the GitHub project. Lauch a Command Propmt and run the following commands:
```
% cd c:\temp 
% git clone https://github.com/ibm-watson-iot/mas-scada-bulkingest
```


## Install on Host Operating System

### On macOS or Linux system

Open a shell prompt, and run the install script:
```
$ cd /tmp/mas-scada-bulkingest
$ ./bin/install.sh
```

### On Windows system

Lauch a Command Propmt and run the following commands:
```
% cd c:\temp\mas-scada-bulkingest
% .\bin\install.bat
```


## Build and run docker image

You can also build and run container on any operating environment that has docker-ce
(Docker Community Edition) environment set.  For information on Docker Community
Edition for your operating environment, see [About Docker CE](https://docs.docker.com/install/).

Instructions in this section is for docker image creation on a Linux or macOS system. If you need to run 
docker container on a Widnows system, you can export docker image,  import and run on a Windows system.

If you have make command available on the system where you are building docker image, change directory to data connector source directory and run the following **make** commands:
```
To build docker image
$ make dockerimage

To run docker container
$ make dockerrun

To remove container and image
$ make dockerreemove
```

You can also use **docker** command to build and run container. From the data connector source directory, run the following **docker** commands:
```
To build docker image
$ docker build -f ./docker/Dockerfile -t mas-dataiingest:1.0

To run docker image (it is recommended to map configuration and data volumns)
$ mkdir -p ~/volume
$ docker run -dit --name mas-dataingest --volume ~/volume:/root/ibm/masdc/volume mas-dataiingest:1.0
```


## Configure data connector

Refer to data connector documentation for data connector configuration:

https://github.com/ibm-watson-iot/mas-scada-bulkingest/blob/master/mkdocs/docs/index.md



