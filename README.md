# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector

This project includes source to build an utility that can be used to bulk ingest data exported
or extracted from SCADA historian in CSV format, to Watson IoT Platform Data Lake.

You can build and install bulk data connector in a Docker image or on an on-premise host system.

## Dependencies

* Python 3.7 or higher
* OpenJDK 13.0.1
* Docker (for dockerized version)
  You can build and run container on any operating environment that has docker-ce
  (Docker Community Edition) environment set.  For information on Docker Community
  Edition for your operating environment, see [About Docker CE](https://docs.docker.com/install/). 


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

https://ibm-watson-iot.github.io/mas-scada-bulkingest/


