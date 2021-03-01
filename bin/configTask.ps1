#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
# *****************************************************************************
# Copyright (c) 2020-2021 IBM Corporation and other Contributors.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Powershell Script to configure IBM MAS Data Connector tasks on Windows Servers
#
# To run this script, you need Admin priviledges. Open a Windows command propmt
# with Admin privilege and run the following command:
#
# c:> powershell.exe -ExecutionPolicy Bypass .\bin\configTask.ps1
#

# Update these variables if required
$InstallPath = "C:\IBM\masdc"
$DataPath = "C:\IBM\masdc"

# ----------------------------------------------------------------------------
#           Do not make any changes beyond this line
#

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
    <Description>Uploads device data from SCADA historian to IBM MAS for Monitoring</Description>
    <URI>\IBM\Maximo Application Suite\IBMMASDeviceDataUpload</URI>
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
      <Command>c:\ibm\masdc\jre\bin\java.exe</Command>
      <Arguments>-classpath "c:\ibm\masdc\jre\lib\*;c:\ibm\masdc\lib\*" com.ibm.wiotp.masdc.Connector device</Arguments>
      <WorkingDirectory>C:\IBM\masdc\volume\data</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# Register entity upload task
$taskName = "IBMMASDeviceDataUpload"
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
    <URI>\IBM\Maximo Application Suite\IBMMASAlarmDataUpload</URI>
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
      <Command>c:\ibm\masdc\jre\bin\java.exe</Command>
      <Arguments>-classpath "c:\ibm\masdc\jre\lib\*;c:\ibm\masdc\lib\*" com.ibm.wiotp.masdc.Connector alarm</Arguments>
      <WorkingDirectory>C:\IBM\masdc\volume\data</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# Register Alarm upload task
$taskName = "IBMMASAlarmDataUpload"
$taskPath = "\IBM\Maximo Application Suite"
Register-ScheduledTask -Xml $xmlalarm -TaskName $taskName -TaskPath $taskPath


