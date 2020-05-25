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
def set_timestamp_field(row, tStamp, regreq):
    ts = row[tStamp]
    tsUTC = getTS(ts, regreq)
    return tsUTC 

# Function to set device ID field
def set_deviceid_field(row, idcolName, prefix, format):
    if prefix != '':
        if format != '':
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



#
# Function to normalize extracted data based on configuration
#
def normalizeDataFrame(dataPath, inputFile, config, regreq):
    logger.info("TransformInput: File: " + inputFile)

    # Create data frame from input file
    df = pd.read_csv(dataPath+'/csv/'+inputFile)
    print(df)

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

            if tagData["tagpathParseCount"] > 0 and len(tagData["tapMap"]) > 0:
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
            
                if len(tagData['tagMap']) > 0:
                    logger.info("Apply Tagpath Mapping rules")
                    df.rename(columns=tagData['tagMap'], inplace=True)

    # Set device type and id
    if 'entityData' in config:
        entityData = config['entityData']
        deviceType = ''
        if 'deviceType' in entityData:
            deviceType = entityData['deviceType']
        if deviceType != '':
            df['deviceType'] = df[deviceType]
        else:
            if 'setType' in entityData:
                setType = entityData['setType']
                df['deviceType'] = setType
            else:
                df['deviceType'] = config['client'] + "Type"
        deviceId = ''
        if 'deviceId' in entityData:
            deviceId = entityData['deviceId']
        if deviceId != '':
            deviceIdPrefix = ""
            if 'deviceIdPrefix' in entityData:
                deviceIdPrefix = entityData['deviceIdPrefix']
            deviceIdFormat = ""
            if 'deviceIdFormat' in entityData:
                deviceIdFormat = entityData['deviceIdFormat']
            df['deviceId'] = df.apply (lambda row: set_deviceid_field(row, deviceId, deviceIdPrefix, deviceIdFormat), axis=1)
        else:
            df['deviceId'] = config['client'] + "Id"

    # set dimension data column
    if setDimensions == True and tagpath != '':
        df['dimensionData'] = df.apply (lambda row: set_dimensionData_field(row, tagpath), axis=1)

    # Set event data fields
    if 'eventData' in config:
        eventData = config['eventData']
        # Ignore timestamp column for registration data
        if regreq == 0:
            tStamp = ''
            if 'timestamp' in eventData:
                tStamp = eventData['timestamp']
            if tStamp != "":
                df[tStamp] = df.apply (lambda row: set_timestamp_field(row, tStamp, regreq), axis=1)
        evtId = ''
        if 'id' in eventData:
            evtId = eventData['id']
        if evtId != '':
            df['EVENTID'] = df[evtId]

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

    deviceOptions = {"org": orgid, "type": deviceType, "id": deviceId, "auth-method": "token", "auth-token": token}
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

    # update processed status file
    if sampleEventCount == -1:
        logger.info("Create processed status file: " + str(nevents))
        prname = dataPath+type+'/data/.processed'
        f = open(prname,"w+")
        f.write("{ \"processed\": %d , \"uploaded\":\"Y\" }" % nevents)
        f.close()



