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
# To run this script, you need Admin priviledges. Open a Windows command propmt
# with Admin privilege and run the following command:
#
# c:> powershell.exe -ExecutionPolicy Bypass .\bin\install.ps1
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
    New-Item -Path "$InstallPath" -ItemType Directory
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
    New-Item -Path "$DataPath" -ItemType Directory
    New-Item -Path "$DataPath\volume\logs" -ItemType Directory
    New-Item -Path "$DataPath\volume\config" -ItemType Directory
    New-Item -Path "$DataPath\volume\data" -ItemType Directory
}

# Set Environment variables
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_INSTALL_DIR', $InstallPath,[System.EnvironmentVariableTarget]::Machine)
[System.Environment]::SetEnvironmentVariable('IBM_DATAINGEST_DATA_DIR', $DataPath,[System.EnvironmentVariableTarget]::Machine)

# Schedule upload tasks
$scheduleObject = New-Object -ComObject schedule.service
$scheduleObject.connect()
$rootFolder = $scheduleObject.GetFolder("\")
$rootFolder.CreateFolder("IBM")
$ibmFolder = $scheduleObject.GetFolder("\IBM")
$ibmFolder.CreateFolder("Maximo Application Suite")

# Entity data upload task
# Task config xml
$xmlentity = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>IBM</Author>
    <Description>Uploads entity data from SCADA historian to IBM MAS for Monitoring</Description>
    <URI>\IBM\Maximo Application Suite\Entity Data Upload Task</URI>
  </RegistrationInfo>
  <Triggers>
    <RegistrationTrigger>
      <Repetition>
        <Interval>P31D</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <Enabled>true</Enabled>
    </RegistrationTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>S-1-5-18</UserId>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>true</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <DisallowStartOnRemoteAppSession>false</DisallowStartOnRemoteAppSession>
    <UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT5M</Interval>
      <Count>3</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>cmd</Command>
      <Arguments>/K "C:\Program Files\IBM\masdc\bin\connector.bat" entity</Arguments>
      <WorkingDirectory>C:\IBM\masdc\volume\logs</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# Register entity upload task
$taskName = "IBMMASEntityDataUpload"
$taskPath = "\IBM\Maximo Application Suite"
Register-ScheduledTask -Xml $xmlentity -TaskName $taskName -TaskPath $taskPath

# Alarm data upload task
# Task config xml
$xmlalarm = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>IBM</Author>
    <Description>Uploads alarm data from SCADA historian to IBM MAS for Monitoring</Description>
    <URI>\IBM\Maximo Application Suite\Alarm Data Upload Task</URI>
  </RegistrationInfo>
  <Triggers>
    <RegistrationTrigger>
      <Repetition>
        <Interval>P31D</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <Enabled>true</Enabled>
    </RegistrationTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>S-1-5-18</UserId>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>true</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <DisallowStartOnRemoteAppSession>false</DisallowStartOnRemoteAppSession>
    <UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT5M</Interval>
      <Count>3</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>cmd</Command>
      <Arguments>/K "C:\Program Files\IBM\masdc\bin\connector.bat" alarm</Arguments>
      <WorkingDirectory>C:\IBM\masdc\volume\logs</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# Register Alarm upload task
$taskName = "IBMMASAlarmDataUpload"
$taskPath = "\IBM\Maximo Application Suite"
Register-ScheduledTask -Xml $xmlalarm -TaskName $taskName -TaskPath $taskPath


