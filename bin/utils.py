#!/usr/bin/python3
#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
#
# *****************************************************************************
# Copyright (c) 2019 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Utility functions
#

import argparse
import json
import csv
import datetime
import pandas as pd
import ibmiotf.api
import ibmiotf.device
from pathlib import Path
import logging
import sys, subprocess
import os
from os import path
import uuid


userHome = str(Path.home())
defaultDir = userHome + "/ibm/masdc"
installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)
dibin = installDir + "/bin"
sys.path.append(dibin)

logger = logging.getLogger('dataingest.prepare')

# Create directory in users home directory
def createDir(dataDir, dirname, permission):
    if dirname == "":
        logger.info("Specified directory name is empty.")
        return 1

    if permission == "":
        permission = 0o755

    dpath = dataDir + "/" + dirname

    try:
        os.mkdir(dpath.strip(), permission)
    except FileExistsError:
        logger.warning("The directory %s exists." % dpath)
    except OSError:
        logger.error("Failed to create the directory: %s" % dpath)
        return 1
    else:
        logger.info("The directory %s is created." % dpath)

    return 0


# Set value from datatype field 
def set_value_from_datatype(row):
    if row['datatype'] == 0 :
        return row["intvalue"]
    if row['datatype'] == 1 :
        return row["floatvalue"]
    if row['datatype'] == 2 :
        return row["stringvalue"]
    if row['datatype'] == 4 :
        return row["datevalue"]
    return 'Unknown'


# Set data type as string
def set_datatype_field(row):
    if row['datatype'] == 0 :
        return "int"
    if row['datatype'] == 1 :
        return "float"
    if row['datatype'] == 2 :
        return "string"
    if row['datatype'] == 4 :
        return "date"
    return str(row[tType])

# Function to get Timestamp in ISO8601 format    
def getTS(ts, regreq):
    utc = ts
    tstamp = int(ts)
    if tstamp != 0:
        s = tstamp / 1000.0
        if regreq == 1:
            utc = datetime.datetime.utcfromtimestamp(s).isoformat() + '+00:00'
        else:
            utc = datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S')
    return utc

# Set timestamp column
def set_timestamp_field(row, tStamp, tsConvert, regreq):
    ts = row[tStamp]
    if tsConvert == 0:
        return ts
    tsUTC = getTS(ts, regreq)
    return tsUTC 

# Function to set device ID field
def set_deviceid_field(row, idcolName, prefix, format, tagpathColName):
    if prefix != '':
        if format != '':
            if format == 'UUID':
                id = prefix + "_" + row[idcolName].replace('-', '')
            elif format == 'UUID5':
                id = str(uuid.uuid5(uuid.NAMESPACE_DNS, prefix + "/" + row[tagpathColName]))
            else:
                id = prefix + "_" + str(format % row[idcolName]).replace(' ', '0')
        else:
            id = prefix + "_" + row[idcolName]
    else:
        id = row[idcolName].replace(' ', '_').replace('/', '_')
    return id

# Function to set int value
def set_int_value(row, colName):
    if row[colName] == "":
        return 0
    else:
        row[colName]

# Function to set float value
def set_float_value(row, colName):
    if row[colName] == "":
        return 0.0
    else:
        row[colName]

# Function to set event id column
def set_eventid_value(row, colName, level):
    return row[colName].rsplit("/",1)[1]

# Function to set dimension data field - merge device id and tagpath
def set_dimensionData_field(row, idcolName):
    id = str(row['deviceId']) + "#" + str(row[idcolName])
    return id

# Function to set empty value from alternate column value
def set_alternate_value(row, fromColName, toColName):
    if row[toColName] == '' or pd.isnull(row[toColName]):
        return row[fromColName]
    return row[toColName]

#
# Function to normalize extracted data based on configuration
#
def normalizeDataFrame(dataPath, inputFile, config, regreq):
    logger.info("TransformInput: File: " + inputFile)

    # Create data frame from input file
    df = pd.read_csv(dataPath+'/csv/'+inputFile)
    print(df)

    # Rename columns
    if 'renameColumns' in config:
        if len(config['renameColumns']) > 0:
            logger.info("Apply column rename rules")
            df.rename(columns=config['renameColumns'], inplace=True)

    # process tagpath if set
    setDimensions = False
    tagpath = ""
    if 'tagData' in config:
        tagData = config['tagData']
        if 'tagpath' in tagData:
            tagpath  = tagData["tagpath"]
        if 'setDimensions' in tagData:
            setDimensions  = tagData["setDimensions"]
        if tagpath != "":
            # Get event from tagpath if configured
            if tagData['eventTagLevel'] > 0:
                tagLevel = tagData['eventTagLevel']
                evtName = tagData['eventColumnName']
                df['evt_name'] = df.apply (lambda row: set_eventid_value(row, tagpath, tagLevel), axis=1) 

            if 'tagpathParseCount' in tagData:
                if  tagData["tagpathParseCount"] > 0:
                    # process tagpath
                    tags = tagpath.split("/", n=0, expand=True)
                    tagindex = 0
                    for tagid in tags.iloc[0]:
                        tagid = "t_dim" + str(tagindex)
                        df[tagid] = tags[tagindex]
                        tagindex += 1
        
                    rindex = tagData["tagpathParseCount"] - tagindex
                    if rindex > 0:
                        for i in range(rindex):
                            tagid = "t_dim" + str(tagindex + i)
                            df[tagid] = " "
            
                if 'tagMap' in tagData:
                    if len(tagData["tapMap"]) > 0:
                        logger.info("Apply Tagpath Mapping rules")
                        df.rename(columns=tagData['tagMap'], inplace=True)

    # Set device type and id
    if 'entityData' in config:
        entityData = config['entityData']
        deviceType = ''
        if 'deviceType' in entityData:
            deviceType = entityData['deviceType']
        if deviceType != '':
            df['deviceType'] = deviceType
        else:
            if config['type'] == "entity":
                entityData['setType'] = config['client'] + "_Device_Type"
            else:
                entityData['setType'] = config['client'] + "_Alarm_Type"
            if 'setType' in entityData:
                setType = entityData['setType']
                df['deviceType'] = setType
            else:
                df['deviceType'] = config['client'] + "Type"
        deviceId = ''
        if 'deviceId' in entityData:
            deviceId = entityData['deviceId']
        if deviceId != '':
            deviceIdPrefix = config['client']
            if 'deviceIdPrefix' in entityData:
                deviceIdPrefix = entityData['deviceIdPrefix']
            deviceIdFormat = "UUID5"
            if 'deviceIdFormat' in entityData:
                deviceIdFormat = entityData['deviceIdFormat']
            df['deviceId'] = df.apply (lambda row: set_deviceid_field(row, deviceId, deviceIdPrefix, deviceIdFormat, tagpath), axis=1)
        else:
            df['deviceId'] = config['client'] + "Id"

    # set dimension data column
    if tagpath != '':
        df['dimensionData'] = df.apply (lambda row: set_dimensionData_field(row, tagpath), axis=1)

    # Set event data fields
    if 'eventData' in config:
        eventData = config['eventData']
        # Ignore timestamp column for registration data
        if regreq == 0:
            tStamp = ''
            tsConvert = 1
            if 'timestamp' in eventData:
                tStamp = eventData['timestamp']
            if 'tsconvert' in eventData:
                if eventData['tsconvert'] == False:
                    tsConvert = 0
            if tStamp != "":
                df[tStamp] = df.apply (lambda row: set_timestamp_field(row, tStamp, tsConvert, regreq), axis=1)
        # Set event Id
        evtId = ''
        if 'id' in eventData:
            evtId = eventData['id']
        if evtId != '':
            df['EVENTID'] = df[evtId]
        # Update empty field of a column from alternate column field 
        # Example - for alarm data, populate name from almname 
        if 'dataAltMap' in eventData:
            dataAltMap = eventData['dataAltMap']
            for fromColName, toColName in dataAltMap.items():
                if fromColName != '' and toColName != '':
                    df[toColName] = df.apply (lambda row: set_alternate_value(row, fromColName, toColName), axis=1)

    # Process discardColumns
    discardColumns = config['discardColumns']
    df = df.drop(discardColumns, axis=1)

    # Change NaN 
    evtCols = config['interfaceEvents']
    if len(evtCols) > 0:
        for key, value in evtCols.items():
            fieldName = str(key)
            fieldType = str(value)
            if fieldType == 'integer':
                df[fieldName].fillna(0, inplace=True)
                df[fieldName] = df[fieldName].astype('int')
            elif fieldType == 'number':
                df[fieldName].fillna(0.0, inplace=True)
                df[fieldName] = df[fieldName].astype('float')
            elif fieldType == 'string':
                df[fieldName].fillna('', inplace=True)
                df[fieldName] = df[fieldName].astype('str')
            else:
                df[fieldName].fillna('', inplace=True)
                df[fieldName] = df[fieldName].astype('str')
    else:
        df.fillna("", inplace=True)


    # Process table (in place) if column maps are not defined
    rowsProcessed = 0
    logger.info("Total number of rows: " + str(len(df)))

    return df


#
# Send sample event - for auto creation of config in data lake
#
def sendEvent(type, conncfg, df, dataPath, useDeviceId, registerSampleEvent, sampleEventCount):
    wiotp = conncfg['wiotp']
    orgid = wiotp["orgId"]
    deviceType = type
    if useDeviceId != "":
        deviceId = useDeviceId
        dropDevIDCol = { 'deviceId' }
        df = df.drop(dropDevIDCol, axis=1)
    else:
        deviceId = type + "Id"
    token = wiotp["token"]

    deviceOptions = {"org": orgid, "type": deviceType, "id": deviceId, "auth-method": "token", "auth-token": token, "port": 443}
    client = ibmiotf.device.Client(deviceOptions)

    try:
        client.connect()
        # Send one sample message
        nevents = 0
        waitcounter = 0
        for row in df.values:
            myData = dict((colname, row[i]) for i, colname in enumerate(df.columns))
            # logger.info(myData)
            print(myData)
            client.publishEvent(deviceType+"Event", "json", myData, qos=1, on_publish=None)
            waitcounter += 1
            nevents += 1
            if sampleEventCount >= 0:
                break
            if waitcounter >= 100:
                logger.debug("Sent 100 events")
                time.sleep(1)
                waitcounter = 0
                continue

        if registerSampleEvent == 1:
            sendEventFD = open(dataPath+'/'+type+'/schemas/.sampleEventSent', "w")
            sendEventFD.write('Sample Event is sent')
            sendEventFD.close()

        client.disconnect()

    except Exception as exc:
        logger.info(exc.response.json())

    if sampleEventCount == -1:
        logger.info("Create processed status file: " + str(nevents))


def getNextExtractionDate(startDate, lastEndTS, nCycle):
    cDate = datetime.datetime.today()
    cDay = cDate.day
    cMonth = cDate.month
    cYear = cDate.year
    lDate = 0
    lDay = 1
    lMonth = 0
    lYear = 0
    nDay = 1
    nMonth = 0
    nYear = 0

    logger.info("Extract startDate:%s lastEndTS:%d nCycle:%d " % (startDate, lastEndTS, nCycle))

    if startDate != '':
        lDate = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
    else:
        lDate = cDate

    if lastEndTS == 0:
        nMonth = lDate.month
        nYear = lDate.year
    elif lastEndTS == -1:
        nYear = lDate.year
        nMonth = lDate.month + 1
        if nMonth > 12:
            nMonth = 1
            nYear = lDate.year + 1
    else:
        lDate = datetime.datetime.fromtimestamp(lastEndTS/1000)
        lMonth = lDate.month
        lYear = lDate.year
        nDate = datetime.datetime.fromtimestamp((lastEndTS/1000)+5)
        nMonth = nDate.month
        nYear = nDate.year
        if nCycle > 0 and nYear < cYear:
            nCycle = 0
            nMonth += 1
            if nMonth > 12:
                nMonth = 1
                nYear += 1


    if nYear > cYear:
        nYear = cYear
        nMonth = cMonth
    elif nYear == cYear:
        if nMonth > cMonth:
            nMonth = cMonth
            
    return nDay,nMonth,nYear,nCycle


