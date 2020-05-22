#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
# *****************************************************************************
# Copyright (c) 2019 IBM Corporation and other Contributors.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Script to install Data Connector on Windows Servers
#


# Create installation directory
Write-Host "Creating Installation directory 'C:\Program Files\IBM\masdc'"
$path = "C:\Program Files\IBM\masdc"
if(!(Test-Path $path))
{
    New-Item -Path 'C:\Program Files\IBM\masdc' -ItemType Directory
}

# Download connector github project
Write-Host "Downloading Connector code"
$path = ".\connector.zip"
if(!(Test-Path $path))
{
    Invoke-WebRequest -Uri "https://github.com/ibm-watson-iot/mas-scada-bulkingest/archive/master.zip" -OutFile ".\connector.zip"
}

# Download JRE
Write-Host "Downloading JRE"
$path = ".\jre.zip"
if(!(Test-Path $path))
{
    Invoke-WebRequest -Uri "https://dataingest.s3.us-south.cloud-object-storage.appdomain.cloud/jre.zip" -OutFile ".\jre.zip"
}

# Download embedded python
Write-Host "Downloading Embedded Python with required packages"
$path = ".\python.zip"
if(!(Test-Path $path))
{
    Invoke-WebRequest -Uri "https://dataingest.s3.us-south.cloud-object-storage.appdomain.cloud/python-3.7.5.zip" -OutFile ".\python.zip"
}

# Expand Connector code
Write-Host "Expanding connector.zip, and copying binaries and libraries in C:\Program Files\IBM\masdc"
$path = ".\connector\mas-scada-bulkingest-master"
if(!(Test-Path $path))
{
    Expand-Archive -Path connector.zip
}
$path = "C:\Program Files\IBM\masdc\bin"
if(!(Test-Path $path))
{
    Copy-Item -Path .\connector\mas-scada-bulkingest-master\bin -Recurse -Destination "C:\Program Files\IBM\masdc\bin"
    Copy-Item -Path .\connector\mas-scada-bulkingest-master\lib -Recurse -Destination "C:\Program Files\IBM\masdc\lib"
}

# Expand jre
Write-Host "Expanding jre.zip in C:\Program Files\IBM\masdc"
$path = "C:\Program Files\IBM\masdc\jre"
if(!(Test-Path $path))
{
    Expand-Archive -Path jre.zip -DestinationPath "C:\Program Files\IBM\masdc"
}

# Expand python
Write-Host "Expanding python.zip in C:\Program Files\IBM\masdc"
$path = "C:\Program Files\IBM\masdc\python-3.7.5"
if(!(Test-Path $path))
{
    Expand-Archive -Path python.zip -DestinationPath "C:\Program Files\IBM\masdc"
}

# Create Data dir
# Create installation directory
Write-Host "Creating Installation directory 'C:\IBM\masdc'"
$path = "C:\IBM\masdc"
if(!(Test-Path $path))
{
    New-Item -Path 'C:\IBM\masdc' -ItemType Directory
}

# Set Environment variables
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_INSTALL_DIR', 'C:\Program Files\IBM\masdc',[System.EnvironmentVariableTarget]::Machine)
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_DATA_DIR', 'C:\IBM\masdc',[System.EnvironmentVariableTarget]::Machine)


# Set service
Write-Host "Set IBM MAS Dataconnector service"
$serviceName = "IBMMasConnectorService"

if (Get-Service $serviceName -ErrorAction SilentlyContinue)
{
    $serviceToRemove = Get-WmiObject -Class Win32_Service -Filter "name='$serviceName'"
    $serviceToRemove.delete()
    Write-Host "Service $serviceName is removed"
}
else
{
    Write-Host "Service $serviceName does not exists"
}

Write-Host "Creating service $serviceName"
$binaryPath = "C:\Program Files\IBM\masdc\bin\run.bat CakebreadType restart"
New-Service -name $serviceName -binaryPathName $binaryPath -displayName $serviceName -startupType Automatic

Write-Host "Service $serviceName installation is created."


