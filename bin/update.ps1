#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
# *****************************************************************************
# Copyright (c) 2021 IBM Corporation and other Contributors.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Powershell Script to update IBM MAS Data Connector to a newer version on Windows Servers
#
# To run this script, you need Admin priviledges. Open a Windows command propmt
# with Admin privilege and run the following command:
#
# c:> powershell.exe -ExecutionPolicy Bypass .\bin\update.ps1
#

$ConnectorJarVersion = "1.0.1"

if ( $args.count -eq 1 ) {
    $ConnectorJarVersion = $args[0]
}

$InstallPath = "C:\IBM\masdc"
$TmpPath = "C:\IBM\masdc\volume\tmp"

Write-Host "Creating tmp directory"
if(!(Test-Path $TmpPath))
{
    New-Item -Path "$TmpPath" -ItemType Directory
}

cd c:\ibm\masdc\volume\tmp

# stop tasks
Write-Host "Stop Tasks"
$alarmTaskName = "IBMMASAlarmDataUpload"
$deviceTaskName = "IBMMASDeviceDataUpload"
$taskPath = "\IBM\Maximo Application Suite\"
Stop-ScheduledTask -TaskName $alarmTaskName -TaskPath $taskPath
Stop-ScheduledTask -TaskName $deviceTaskName -TaskPath $taskPath
Write-Host "Show Tasks"
Get-ScheduledTask -TaskName $alarmTaskName -TaskPath $taskPath
Get-ScheduledTask -TaskName $deviceTaskName -TaskPath $taskPath

# Download updated jar
Write-Host "Downloading updated jar"
$jarName = "mas-dataconnector-" + $ConnectorJarVersion + ".jar"
$DownloadUrl = "https://dataingest.s3.us-south.cloud-object-storage.appdomain.cloud/" + $jarName
$path = ".\" + $jarName
if(!(Test-Path $path))
{
    Invoke-WebRequest -Uri "$($DownloadUrl)" -OutFile "$($path)"
}
 
if((Test-Path $path))
{
    del "$InstallPath\lib\mas-dataconnector-*.jar"
    Copy-Item -Path "$($path)" -Destination "$InstallPath\lib"
}

# start tasks
Start-ScheduledTask -TaskName $alarmTaskName -TaskPath $taskPath
Start-ScheduledTask -TaskName $deviceTaskName -TaskPath $taskPath

