#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
# *****************************************************************************
# Copyright (c) 2020 IBM Corporation and other Contributors.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Windows Powershell Script to install IBM MAS Data Connector on Windows Servers
#

# Update these variables if required
$InstallPath = "C:\Program Files\IBM\masdc"
$DataPath = "C:\IBM\masdc"

# ----------------------------------------------------------------------------
#           Do not make any changes beyond this line
#

# Create installation directory
Write-Host "Creating Installation directory $InstallPath"
if(!(Test-Path $InstallPath))
{
    New-Item -Path '$InstallPath' -ItemType Directory
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
$path = "$InstallPath\bin"
if(!(Test-Path $path))
{
    Copy-Item -Path .\connector\mas-scada-bulkingest-master\bin -Recurse -Destination "$InstallPath\bin"
    Copy-Item -Path .\connector\mas-scada-bulkingest-master\lib -Recurse -Destination "$InstallPath\lib"
}

# Expand jre
Write-Host "Expanding jre.zip in $InstallPath"
$path = "$InstallPath\jre"
if(!(Test-Path $path))
{
    Expand-Archive -Path jre.zip -DestinationPath "$InstallPath"
}

# Expand python
Write-Host "Expanding python.zip in $InstallPath"
$path = "$InstallPath\python-3.7.5"
if(!(Test-Path $path))
{
    Expand-Archive -Path python.zip -DestinationPath "$InstallPath"
}

# Create Data dir
Write-Host "Creating Data directory $DataPath"
if(!(Test-Path $DataPath))
{
    New-Item -Path '$DataPath' -ItemType Directory
}

# Set Environment variables
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_INSTALL_DIR', '$InstallPath',[System.EnvironmentVariableTarget]::Machine)
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_DATA_DIR', '$DataPath',[System.EnvironmentVariableTarget]::Machine)


# Set service to extract entity data
Write-Host "Set IBM MAS Entity Data Upload Service"
$serviceName = "IBMMASEntityConnectorService"
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
$binaryPath = "$InstallPath\bin\connector.bat entity"
New-Service -name $serviceName -binaryPathName $binaryPath -displayName $serviceName -startupType Automatic
Write-Host "Service $serviceName installation is created."


# Set service to extract alarm data
Write-Host "Set IBM MAS Alarm Data Upload Service"
$serviceName = "IBMMASAlarmConnectorService"
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
$binaryPath = "$installPath\bin\connector.bat alarm"
New-Service -name $serviceName -binaryPathName $binaryPath -displayName $serviceName -startupType Automatic
Write-Host "Service $serviceName installation is created."

