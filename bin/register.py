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
# Registers device type, device id, and interfaces, and send a sample
# event to WIoTP for device type table to get created in Data Lake.
#


import argparse
import yaml
import json
import csv
import datetime
import pandas as pd
import ibmiotf.api
import ibmiotf.device
from os import path
import logging
from pathlib import Path
import sys, subprocess
import os
import time


userHome = str(Path.home())
defaultDir = userHome + "/ibm/masdc"
installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)
dibin = installDir + "/bin"
sys.path.append(dibin)
import utils


logger = logging.getLogger('dataingest.register')



#
# Get activated logical interface id
#
def getLogicalInterfaceId(type, conncfg):
    # Get parameters from config object
    wiotp = conncfg['wiotp']
    key = wiotp["key"]
    token = wiotp["token"]
  
    params = {"auth-key": key, "auth-token": token}
    api = ibmiotf.api.ApiClient(params)

    retid = ""
    try:
        resp = api.getLogicalInterfacesOnDeviceType(type)
        version = resp[0]["version"]
        if version == "active":
            return resp[0]["id"]
        else:
            return retid
    except:
        logger.info("Failed to get logical interfaces for type " + type)
        return retid

#
# Add device type
#
def addDeviceType(type, conncfg):
    # Get parameters from config object
    deviceType = type
    wiotp = conncfg['wiotp']
    key = wiotp["key"]
    token = wiotp["token"]

    params = {"auth-key": key, "auth-token": token}
    api = ibmiotf.api.ApiClient(params)

    try:
        description = "Device type " + deviceType
        typeData = {"id":deviceType, "description":description}
        logger.info("Register Device Type: " + json.dumps(typeData))
        result = api.registry.devicetypes.create(typeData)
        logger.info(result)
    except Exception as e:
        logger.info("Failed to add device type: " + str(e))


#
# Add devices
#
def addDevices(type, conncfg, df):
    # Get parameters from config object
    deviceType = type
    wiotp = conncfg['wiotp']
    key = wiotp["key"]
    token = wiotp["token"]

    params = {"auth-key": key, "auth-token": token}
    api = ibmiotf.api.ApiClient(params)

    dids = df['deviceId'].unique().tolist()
    for did in dids:
        try:
            deviceData = {"typeId": deviceType, "deviceId": did, "authToken": token, "deviceInfo": {}, "location": {}, "metadata": {}}
            logger.info("Register Device: " + json.dumps(deviceData))
            result = api.registry.devices.create(deviceData)
            logger.info(result)
        except Exception as e:
            logger.info("Failed to add device: " + str(e))


#
# Create interfaces
#
def configureInterfaces(type, conncfg, dataPath, useDeviceId):
    # Get parameters from config object
    deviceType = type
    wiotp = conncfg['wiotp']
    if useDeviceId != "":
        deviceId = useDeviceId
    else:
        deviceId = config.deviceIdPrefix + "_001"
    key = wiotp["key"]
    token = wiotp["token"]
  
    params = {"auth-key": key, "auth-token": token}
    api = ibmiotf.api.ApiClient(params)
  
    ids = {}
    info = {}
    meta = {}
    location = {}
    logicalInterfaceId = ""
  
    evtSchemaFilePath = dataPath + "/" + deviceType + "/schemas/" + deviceType + "EventSchema.json"
    LIFilePath = dataPath + "/" + deviceType + "/schemas/" + deviceType + "LISchema.json"
    evtMappingsFilePath = dataPath + "/" + deviceType + "/schemas/" + deviceType + "EventMappings.json"
  
    logger.info("Add device event schema: " + evtSchemaFilePath)
    infile = open(evtSchemaFilePath)
    schemaFileContents = ''.join([x.strip() for x in infile.readlines()])
    infile.close()
    ids["schema"], result = api.createSchema(deviceType+"EventSchema", deviceType+'EventSchema.json', schemaFileContents)
  
    logger.info("Add event type")
    ids["eventtype"], result = api.createEventType(deviceType+"Event", ids["schema"], deviceType+"Event")
  
    logger.info("Add physical interface")
    ids["physicalinterface"], result = api.createPhysicalInterface(deviceType, "The physical interface for "+deviceType)
  
    logger.info("Add event type to the physical interface")
    result = api.createEvent(ids["physicalinterface"], ids["eventtype"], deviceType+"Event")
  
    logger.info("Update device type to connect to the physical interface")
    result = api.addPhysicalInterfaceToDeviceType(deviceType, ids["physicalinterface"])
  
    logger.info("Add logical interface schema: " + LIFilePath)
    infile = open(LIFilePath)
    schemaFileContents = ''.join([x.strip() for x in infile.readlines()])
    infile.close()
    ids["logicalschema"], result = api.createSchema(deviceType+"LISchema", deviceType+'LISchema.json', schemaFileContents)
    logger.info("Logical interface schema id " + ids["logicalschema"])
  
    logger.info("Add logical interface")
    try:
        ids["logicalinterface"], result = api.createLogicalInterface("Logical Interface for "+deviceType, ids["logicalschema"])
        logicalInterfaceId = str(ids["logicalinterface"])
    except Exception as exc:
        logger.info(exc.response.json())
  
    logger.info("Associate logical interface with the device type")
    result = api.addLogicalInterfaceToDeviceType(deviceType, ids["logicalinterface"])
  
    logger.info("Add mappings to the device type: " + evtMappingsFilePath)
    infile = open(evtMappingsFilePath)
    mappings = json.loads(''.join([x.strip() for x in infile.readlines()]))
    infile.close()
    try:
        result = api.addMappingsToDeviceType(deviceType, ids["logicalinterface"], mappings, notificationStrategy="on-every-event")
    except Exception as exc:
        logger.info(exc.response.json())
  
    logger.info("Validate definitions")
    result = api.validateDeviceTypeConfiguration(deviceType)
    logger.info(result)
    if "is valid" in str(result["message"]):
        logger.info("Activate definitions")
        result = api.activateDeviceTypeConfiguration(deviceType)
        logger.info(result)
        if "successfully submitted for activation" in str(result["message"]):
            intfDoneFile = dataPath+'/'+type+'/schemas/intfActivated'
            intfDoneFD = open(intfDoneFile, "w")
            intfDoneFD.write(logicalInterfaceId)
            intfDoneFD.close()

    return logicalInterfaceId

#
# Function to get list of unique events, and data types
#
def getEventsAndDataTypes(dataPath, config, type):
    csvFile = dataPath+'/csv/'+type+'.csv'
    logger.info("getEventsAndDataTypes: File: " + csvFile)
    tdf = pd.read_csv(csvFile, nrows=10000, engine='c')

    etypes = []
    evtCols = []
    dataTypes = []

    evtNameCol = ''
    evtDataTypeCol = ''
    evtTSCol = ''
    eventData = config['eventData']
    if 'id' in eventData:
        evtNameCol = eventData["id"]
    if 'evtDataType' in eventData['type']:
        evtDataTypeCol = eventData['type']

    if evtNameCol != '':
        etypes = tdf[evtNameCol].unique().tolist()
        for etype in etypes:
            dtype = tdf.loc[tdf[evtNameCol] == etype].iloc[0][evtDataTypeCol]
            if dtype not in eventData['ignoreDataType']:
                dataTypes.append((etype, dtype))
                evtCols.append(etype)

    if 'timestamp' in eventData:
        evtTSCol = eventData["timestamp"]
        evtCols.append(evtTSCol)
        if tdf[evtTSCol].iloc[0].dtype == 'int64':
            dataTypes.append((evtTSCol, "number (epoc)"))
        else:
            dataTypes.append((evtTSCol, "string (date-time)"))

    del tdf
    return evtCols, dataTypes

#
# Function to create Events Mapping JSON file
#
def evtMapCreate(dataPath, config, type, df):
    evtMapFile = dataPath+'/'+type+'/schemas/'+type+'EventMappings.json'
    logger.info("evtMapCreate: File: " + evtMapFile)
    evtMapFD = open(evtMapFile, "w")
    evtMap = {}
    events = {}
    dataTypes = []
    evtCols = config['interfaceEvents']
    if len(evtCols) > 0:
        for key, value in evtCols.items():
            events[str(key)] = '$event.'+str(key)
    else:
        if len(config['renameColumns']) == 0:
            for col in df.columns:
                if 'deviceColumn' in config['entityData']:
                    if str(col) != 'deviceId':
                        events[str(col)] = '$event.'+str(col)
                else:
                    events[str(col)] = '$event.'+str(col)
        else:
            evtCols, dataTypes = getEventsAndDataTypes(dataPath, config, type)
            for evt in evtCols:
                events[str(evt)] = '$event.'+str(evt)

    evtMap[str(type)+'Event'] = events
    json_data = json.dumps(evtMap, indent=4)
    evtMapFD.write(json_data)
    evtMapFD.close()
    return dataTypes

    
#
# Function to create schemas
#
def createSchemas(dataPath, config, conncfg, type, df, dataTypes):
    dataContentDDL = ""
    columnTitles = []
    wiotpCols = ['DEVICETYPE','DEVICEID','LOGICALINTERFACE_ID','EVENTTYPE','FORMAT','RCV_TIMESTAMP_UTC','UPDATED_UTC']
    evtSchemaFile = dataPath+'/'+type+'/schemas/'+type+'EventSchema.json'
    LISchemaFile = dataPath+'/'+type+'/schemas/'+type+'LISchema.json'
    logger.info("evtSchemaCreate: File: " + evtSchemaFile)
    evtSchemaFD = open(evtSchemaFile, "w")
    evtSchema = {}
    evtSchema['$schema'] = 'http://json-schema.org/draft-04/schema#'
    evtSchema['type'] = 'object'
    evtSchema['title'] = 'Event Schema for '+type
    evtSchema['desctiption'] = 'Defines '+type+' events'
    properties = {}
    required = []
    evtCols = config['interfaceEvents']
    evtData = config['eventData']
    tsCol = evtData['timestamp']
    if len(evtCols) > 0:
        for key, value in evtCols.items():
            keyObject = {}
            keyObject['description'] = key
            ddlValue = 'VARCHAR(256)'
            if "string" in value.lower():
                keyObject['type'] = 'string'
                keyObject['default'] = ''
            elif "integer" in value.lower():
                keyObject['type'] = 'number'
                keyObject['default'] = 0
                ddlValue = 'INTEGER'
            elif "number" in value.lower():
                keyObject['type'] = 'number'
                keyObject['default'] = 0.0
                ddlValue = 'DOUBLE'
            elif "date-time" in value.lower():
                keyObject['type'] = 'string'
                keyObject['default'] = '1970-01-01T00:00:00.000000+00:00'
                keyObject['format'] = 'date-time'
                ddlValue = 'TIMESTAMP(12)'
 
            properties[str(key)] = keyObject
            if key in config['interfaceRequired']:
                required.append(key)
            kup = key.upper()
            columnTitles.append(kup)
            dataContentDDL = dataContentDDL + kup + " " + ddlValue + ", "
    else:
        if len(config.columnMaps) == 0:
            skipCol = ''
            if 'deviceColumn' in config.metricMaps:
                skipCol = 'evt_deviceid'
            for col in df.columns:
                if str(col) != skipCol:
                    keyObject = {}
                    key = str(col)
                    keyObject['description'] = key
                    type = str(df[col].dtype)
                    if tsCol == key:
                        keyObject['type'] = 'string'
                        keyObject['default'] = '1970-01-01T00:00:00.000000+00:00'
                        keyObject['format'] = 'date-time'
                        ddlValue = 'TIMESTAMP(12)'
                    else:
                        if "object" in type.lower():
                            if tsCol == key:
                                keyObject['type'] = 'string'
                                # keyObject['default'] = '1970-01-01T00:00:00+00:00'
                                keyObject['default'] = '1970-01-01T00:00:00.000000+00:00'
                                keyObject['format'] = 'date-time'
                                ddlValue = 'TIMESTAMP(12)'
                            else:
                                keyObject['type'] = 'string'
                                keyObject['default'] = ''
                                ddlValue = 'VARCHAR(256)'
                        elif "int64" in type.lower():
                            keyObject['type'] = 'number'
                            keyObject['default'] = 0
                            ddlValue = 'DOUBLE'
                        else:
                            keyObject['type'] = 'number'
                            keyObject['default'] = 0.0
                            ddlValue = 'DOUBLE'
                    properties[str(key)] = keyObject
                    required.append(key)
                    kup = key.upper()
                    columnTitles.append(kup)
                    dataContentDDL = dataContentDDL + kup + " " + ddlValue + ", "
        else:
            for dt in dataTypes:
                keyObject = {}
                key = str(dt[0])
                keyObject['description'] = key
                type = str(dt[1])
                if "int64" in type.lower():
                    keyObject['type'] = 'number'
                    keyObject['default'] = 0
                    ddlValue = 'DOUBLE'
                elif "int64 (epoc)" in type.lower():
                    keyObject['type'] = 'number'
                    # keyObject['default'] = '1970-01-01T00:00:00+00:00'
                    # keyObject['default'] = '1970-01-01T00:00:00.000000+00:00'
                    keyObject['format'] = 'date-time'
                    ddlValue = 'TIMESTAMP(12)'
                elif "boolean" in type.lower():
                    keyObject['type'] = 'boolean'
                    keyObject['default'] = False
                elif "string" in type.lower():
                    keyObject['type'] = 'string'
                    keyObject['default'] = ""
                    ddlValue = 'VARCHAR(256)'
                elif "string (date-time)" in type.lower():
                    keyObject['type'] = 'string'
                    # keyObject['default'] = '1970-01-01T00:00:00+00:00'
                    keyObject['default'] = '1970-01-01T00:00:00.000000+00:00'
                    keyObject['format'] = 'date-time'
                    ddlValue = 'TIMESTAMP(12)'
                else:
                    keyObject['type'] = 'number'
                    keyObject['default'] = 0.0
                    ddlValue = 'DOUBLE'
                properties[str(key)] = keyObject
                required.append(key)
                kup = key.upper()
                columnTitles.append(kup)
                dataContentDDL = dataContentDDL + kup + " " + ddlValue + ", "
    evtSchema['properties'] = properties
    evtSchema['required'] = required
    json_data = json.dumps(evtSchema, indent=4)
    evtSchemaFD.write(json_data)
    evtSchemaFD.close()
    logger.info("LISchemaCreate: File: " + LISchemaFile)
    liSchemaFD = open(LISchemaFile, "w")
    liSchemaFD.write(json_data)
    liSchemaFD.close()

    # Create Temporary DDL file if required to create table in Data Lake
    logger.info("Creating temporary DDL file")
    tableName = type.upper()
    dbcfg = conncfg['datalake']
    schemaName = dbcfg['schema'].upper()
    ddlOutFD = open(dataPath+'/'+type+'/schemas/'+type+'.ddl', "w")
    ddlOutFD.write('CREATE TABLE ' + schemaName.strip() + '.IOT_' + tableName.strip() + ' ( '+dataContentDDL+' DEVICETYPE VARCHAR(64), DEVICEID VARCHAR(256), LOGICALINTERFACE_ID VARCHAR(64), EVENTTYPE VARCHAR(64), FORMAT VARCHAR(32), RCV_TIMESTAMP_UTC TIMESTAMP(12), UPDATED_UTC TIMESTAMP(12) )' )
    ddlOutFD.close()


#
# Main
#
if __name__ == "__main__":

    # Command line options
    type = sys.argv[1]
    if type is None or type == "":
        print("Specify entity type")
        sys.exit()

    print("Start registration cycle for type: " + type)

    userHome = str(Path.home())
    defaultDir = userHome + "/ibm/masdc"
    installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
    dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)

    # Create log directories
    dirperm = 0o755
    retval = utils.createDir(dataDir, "volume", dirperm)
    retval += utils.createDir(dataDir, "volume/logs", dirperm)
    retval += utils.createDir(dataDir, "volume/logs/"+type, dirperm)

    # set log dir
    logdir = dataDir + "/volume/logs/"+type
    logfile = logdir + "/register.log"
    if retval > 0:
        logdir = userHome
        logfile = userHome + "/" + type.strip() + ".log"

    # change working directory to log dir
    os.chdir(logdir)

    # Set logger file handler and level
    hdlr = logging.FileHandler(logfile, mode='a', encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s %(levelno)d: %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)

    # Create directories
    logger.info("Create directory if needed to process: %s", type)
    retval = utils.createDir(dataDir, "volume/data/" + type, dirperm)
    retval += utils.createDir(dataDir, "volume/data/" + type + "/schemas", dirperm)
    retval += utils.createDir(dataDir, "volume/data/" + type + "/data", dirperm)

    # Specify config and data path
    configPath = dataDir + "/volume/config"
    dataPath = dataDir + "/volume/data"

    inputFile = type + ".csv" 
    logger.info("Process input device data file: "+ inputFile)

    # Get configuration
    if configPath == "":
        logger.error("ConfigPath is empty")
        sys.exit()

    # Configuration filess
    tableCfgPath = configPath+'/'+type+'.json'
    connCfgPath = configPath+'/connection.json'
    logger.info("Device type table configuration file: "+ tableCfgPath)
    logger.info("Connection configuration file: "+ connCfgPath)
    if path.exists(tableCfgPath) == False:
        logger.error("Device type table configuration file does not exist.")
        sys.exit()
    if path.exists(connCfgPath) == False:
        logger.error("Connection configuration file does not exist.")
        sys.exit()
    config = {}
    conncfg = {}
    with open(tableCfgPath) as configFD:
        config = json.load(configFD)
        logger.info("Read configuraion items from entity config file.")
        configFD.close()

    with open(connCfgPath) as configFD:
        conncfg = json.load(configFD)
        logger.info("Read configuration items from connection config file.")
        configFD.close()

    # Normalize extracted data
    df = utils.normalizeDataFrame(dataPath, inputFile, config, 1)

    # check if tagpath needs to be set as device ID
    useDeviceId = ''
    if 'entityData' in config:
        if 'deviceId' in config['entityData']:
            useDeviceId = df.iloc[0]['deviceId']
            print("Use deviceId for sending sample event: " + useDeviceId)

    # Create event mappings, phyical and logical interface schemas
    dataTypes = evtMapCreate(dataPath, config, type, df)
    createSchemas(dataPath, config, conncfg, type, df, dataTypes)

    # Check of interface is created for the device type and interface id
    # is cached in the file.
    logicalInterfaceId = ""
    intfDoneFile = dataPath+'/'+type+'/schemas/intfActivated'
    if path.exists(intfDoneFile) == True:
        # get interface id
        intfDoneFD = open(intfDoneFile, "r")
        logicalInterfaceId = intfDoneFD.readline()
        intfDoneFD.close()
        logger.info("Read Logical Interface ID: " + logicalInterfaceId)
    else:
        # get logical interface id for the type
        logicalInterfaceId = getLogicalInterfaceId(type, conncfg)
        if logicalInterfaceId != "":
            logger.info("Write Logical Interface ID: " + logicalInterfaceId)
            intfDoneFD = open(intfDoneFile, "w")
            intfDoneFD.write(logicalInterfaceId)
            intfDoneFD.close()

    # create device type, id, phycal and logical interfacees and activate interface
    eventData = config['eventData']
    if logicalInterfaceId == "" and eventData['registerInterfaces'] == True:
        addDeviceType(type, conncfg)
        addDevices(type, conncfg, df)
        logicalInterfaceId = configureInterfaces(type, conncfg, dataPath, useDeviceId)
        logger.info("Configured Logical Interface ID: " + logicalInterfaceId)

    # Send sample event
    if logicalInterfaceId != "": 
        logger.info("Send sample events")
        if config['mqttEvents'] != 0:
            # check if sample event is already sent
            msgSentFile = dataPath+'/'+type+'/schemas/.sampleEventSent'
            if path.exists(msgSentFile) == False:
                utils.sendEvent(type, conncfg, df, dataPath, useDeviceId, 1, config['mqttEvents'])
                # wait for some time for table in data lake to get created
                time.sleep(5)
        else:
            sendEventFD = open(dataPath+'/'+type+'/schemas/.sampleEventSent', "w")
            sendEventFD.write('Sample Event is sent')
            sendEventFD.close()


