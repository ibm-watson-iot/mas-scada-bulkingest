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
# Transforms and mapps extracted data (from SCADA historian)
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
import requests

userHome = str(Path.home())
defaultDir = userHome + "/ibm/masdc"
installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)
dibin = installDir + "/bin"
sys.path.append(dibin)
import utils

logger = logging.getLogger('dataingest.prepare')


#    
# Function to transform input CSV file, into a format required for 
# bulk data ingestion in WIoTP Data lakedf.fillna(0, inplace=True)
#
def transformInputCSV(dataPath, interfaceId, inputFile, outputFile, type, conncfg, config, columnTitles):
    logger.info("TransformInput: File: " + inputFile)

    # can not proceed if interfaceID is NULL
    if not interfaceId:
        logger.info("ERROR: Interface ID is empty")
        interfaceId = "NotAssigned"

    logger.info("Interface ID: " + interfaceId)

    # get current time in UTC
    # curtime = datetime.datetime.utcnow().isoformat() + '+00.00'
    curtime = datetime.datetime.utcnow().isoformat().replace('T', ' ')

    # Normalize extracted data
    df = utils.normalizeDataFrame(dataPath, inputFile, config, 0)

    # Add devices if not registered yet
    addDevices(type, conncfg, df)
    
    # Add dimensions
    if addDimensions(type, conncfg, config, df) == True:
        discardColumn = [ 'dimensionData' ]
        df = df.drop(discardColumn, axis=1)
    
    # If events are sent using mqtt then return df
    if config['mqttEvents'] == -1:
        rowsProcessed = len(df)
        return rowsProcessed, df

    # Process table
    entityData = config['entityData']
    eventData = config['eventData']
    tStamp = eventData['timestamp']
    columnMaps = config['renameColumns']
    if len(columnMaps) == 0:

        # make column header uppercase
        df.columns = map(str.upper, df.columns)

        # set fixed columns
        if 'DEVICETYPE' not in df.columns:
            df['DEVICETYPE'] = type.strip()
        if 'DEVICEID' not in df.columns:
            df['DEVICEID'] = entityData['deviceIdPrefix']
        df['LOGICALINTERFACE_ID'] = interfaceId
        if 'EVENTTYPE' not in df.columns:
            df['EVENTTYPE'] = type.strip()+"Event"
        df['FORMAT'] = "JSON"
        if tStamp != "":
            tStampCol = tStamp.upper()
            df['RCV_TIMESTAMP_UTC'] = df[tStampCol]
        else:
            df['RCV_TIMESTAMP_UTC'] = curtime
        df['UPDATED_UTC'] = df['RCV_TIMESTAMP_UTC']


        if tStamp != "":
            tStampCol = tStamp.upper()
            discardColumn = [ tStampCol ]
            df = df.drop(discardColumn, axis=1)

        logger.info("Rename Columns:")
        logger.info(columnTitles)
        logger.info(len(columnTitles))
        if len(columnTitles) > 0:
            df = df.reindex(columns=columnTitles)

        # Write updated data frame into a CSV file
        rowsProcessed = len(df)

        logger.info("Transformed File: " + outputFile)
        df.to_csv(outputFile, index = False)

    else:

        # Process table if column maps are defined. Since there is a possibility of 
        # different events sent by the device at the same time, some aggregation is needed.
        # To handle this case, a new dataframe needs to be created.

        columnMaps = config['renameColumns']
        evtNameCol = columnMaps["evtType"]
        evtDataTypeCol = columnMaps["evtDataType"]
        evtTSCol = columnMaps["evtTimestamp"]
        evtValueCol = columnMaps["evtValue"]
        etypes = df[evtNameCol].unique().tolist()
        dtypes = df[evtDataTypeCol].unique().tolist()
        tstamps = df[evtTSCol].unique().tolist()
        evtCols = []
        for etype in etypes:
            dtype = df.loc[df[evtNameCol] == etype].iloc[0][evtDataTypeCol]
            if dtype not in config.ignoreDataType:
                evtCols.append(etype)
        evtCols.append(evtTSCol)

        ndf = pd.DataFrame(columns = evtCols)

        i=0
        # for ts in tstamps:
        while i < len(tstamps)-1:
            ts = tstamps[i]
            sdf = df[df[evtTSCol] == ts]
            rowsProcessed += len(sdf)
            ndf.loc[len(ndf)] = 0
            for index, row in sdf.iterrows():
                if row[evtDataTypeCol] not in config.ignoreDataType:
                    ndf.at[i, row[evtNameCol]] = row[evtValueCol]
            ndf.at[i, evtTSCol] = ts
            i += 1

        ndf['DEVICETYPE'] = type.strip()
        ndf['DEVICEID'] = entityData['deviceIdPrefix']
        ndf['LOGICALINTERFACE_ID'] = interfaceId
        ndf['EVENTTYPE'] = type.strip()+"Event"
        ndf['FORMAT'] = "JSON"
        ndf['RCV_TIMESTAMP_UTC'] = curtime
        ndf['UPDATED_UTC'] = ndf['RCV_TIMESTAMP_UTC']

        # make column header uppercase
        ndf.columns = map(str.upper, ndf.columns)

        # Write updated data frame into a CSV file
        logger.info("Transformed File: " + outputFile)
        if len(columnTitles) > 0:
            ndf = ndf.reindex(columns=columnTitles)
        ndf.to_csv(outputFile, index = False)
        del ndf

    logger.info("Number of rows processed: " + str(rowsProcessed));
    return rowsProcessed, df

#
# Add devices
#
def addDevices(type, conncfg, df):
    # Get parameters from config object
    deviceType = type
    wiotp = conncfg['wiotp']
    orgid = wiotp['orgId']
    key = wiotp['key']
    token = wiotp['token']

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
# Add dimensions
#
def addDimensions(type, conncfg, config, df):
    # check if setDimensions is enabled
    setDimensions = False
    tagpath = ''
    if 'tagData' in config:
        tagData = config['tagData']
        if 'setDimensions' in tagData:
            setDimensions = tagData['setDimensions']
            tagpath = tagData['tagpath']
    if setDimensions == False or tagpath == '':
        return False

    client = config['client']

    # Get parameters from config object
    deviceType = type
    wiotp = conncfg['wiotp']
    key = wiotp["key"]
    token = wiotp["token"]
    tenantId = wiotp["tenantId"]
    geo = wiotp["geo"]

    host = 'https://api-' + geo + '.connectedproducts.internetofthings.ibmcloud.com/api/master/v1/' + tenantId
    api = '/entity/type/' + type + '/categorical'
    url = host + api
    logger.info("URL to register dimensions data: " + url)

    headers = {}
    headers['Content-Type'] = 'application/json'
    headers['x-api-key'] = key
    headers['x-api-token'] = token

    payload = []

    dids = df['dimensionData'].unique().tolist()
    for did in dids:
        dimData = did.split("#")
        idname = dimData[0]
        tpath = dimData[1]
        # Add Client as dimension data
        siteitem = {}
        siteitem['id'] = idname
        siteitem['name'] = "CLIENT"
        siteitem['value'] = client
        payload.append(siteitem)
        # Add complete tagpath as dimension data
        tagpathitem = {}
        tagpathitem['id'] = idname
        tagpathitem['name'] = "TAGPATH"
        tagpathitem['value'] = tpath
        payload.append(tagpathitem)
        # Parse tagpath and add all leaves of tagpath as dimension data
        dims = tpath.split("/")
        for i in range(len(dims)):
            dimname = "LEVEL_" + str(i)
            dimvalue = dims[i]
            # print(dimname, dimvalue)
            item = {}
            item['id'] = idname
            item['name'] = dimname
            item['value'] = dimvalue
            payload.append(item)

    if len(payload) > 0:
        logger.info(json.dumps(payload))
        logger.info("Invoke API to create dimensions.")
        response = requests.post(url, data=json.dumps(payload), params={'blocking': 'true', 'result': 'true'}, headers=headers)
        logger.info(response.status_code)
        logger.info(response.text)

    return True


#
# Main  
#
if __name__ == "__main__":

    # Command line options
    type = sys.argv[1]
    if type is None or type == "":
        print("ERROR: Specify entity type")
        sys.exit()

    applyDDL = False
    if len(sys.argv) > 2:
        ddl = sys.argv[2]
        if ddl == "true":
            applyDDL = True

    # Create log directories
    dirperm = 0o755
    retval = utils.createDir(dataDir, "volume", dirperm)
    retval += utils.createDir(dataDir, "volume/logs", dirperm)
    retval += utils.createDir(dataDir, "volume/logs/"+type, dirperm)

    # set log dir
    logdir = dataDir + "/volume/logs/"+type
    logfile = logdir + "/transform.log"
    if retval > 0:
        logdir = userHome
        logfile = userHome + "/" + type.strip() + ".log"

    # change working directory to log dir
    os.chdir(logdir)

    # Set logger file handler and level
    hdlr = logging.FileHandler(logfile, mode='a', encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s %(lineno)d: %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)

    # Specify config and data path
    configPath = dataDir + "/volume/config"
    dataPath = dataDir + "/volume/data/"

    inputFile = type + ".csv"
    logger.info("Process device type file: "+ inputFile)

    # Get configuration
    if configPath == "":
        print("ERROR: ConfigPath is empty")
        logger.error("ConfigPath is empty")
        sys.exit()

    # Configuration filess
    tableCfgPath = configPath+'/'+type+'.json'
    connCfgPath = configPath+'/connection.json'
    logger.info("Device type table configuration file: "+ tableCfgPath)
    logger.info("Connection configuration file: "+ connCfgPath)
    if path.exists(tableCfgPath) == False:
        print("ERROR: Device type table configuration file does not exist.")
        logger.error("Device type table configuration file does not exist.")
        sys.exit()
    if path.exists(connCfgPath) == False:
        print("ERROR: Connection configuration file does not exist.")
        logger.error("Connection configuration file does not exist.")
        sys.exit()
    config = {}
    conncfg = {}
    with open(tableCfgPath) as configFD:
        logger.info("Load entity type configuration.")
        config = json.load(configFD)
        configFD.close()

    with open(connCfgPath) as configFD:
        logger.info("Load connection configuration.")
        conncfg = json.load(configFD)
        configFD.close()

    # Read column tiles from file
    logger.info("Use table column titles:")
    jsonCols = {}
    try:
        colsFD = open(dataPath+type+'/schemas/'+type+'.dcols', "r")
        jsonCols = json.load(colsFD)
        colsFD.close()
        logger.info(jsonCols["ColumnTitle"])
    except:
        logger.error("Table column names are not extracted from data lake.")
        jsonCols["ColumnTitle"] = []
        logger.info(jsonCols["ColumnTitle"])

    # get interface id
    interfaceId = ""
    intfDoneFile = dataPath+type+'/schemas/intfActivated'
    if path.exists(intfDoneFile) == True:
        # get interface id
        intfDoneFD = open(intfDoneFile, "r")
        interfaceId = intfDoneFD.readline()
        intfDoneFD.close()
        logger.info("Read Logical Interface ID: " + logicalInterfaceId)

    # Transform input CSV file
    outputFile = dataPath+type+'/data/'+type+'.csv'
    if path.exists(outputFile) == True:
        os.remove(outputFile)
    rowsProcessed, df = transformInputCSV(dataPath, interfaceId, inputFile, outputFile, type, conncfg, config, jsonCols["ColumnTitle"])
   
    # If data is uploaded using messaging path, there is no need to bulk upload data
    dtypeProFile = configPath + "/dtype.running"

    if config['mqttEvents'] == -1:

        logger.info("Send events using MQTT")
        utils.sendEvent(config, df, dataPath, 0)

        # update processedi status file
        prname = dataPath+type+'/data/.processed'
        f = open(prname,"w+")
        f.write("{ \"processed\": %d , \"uploaded\":\"Y\" }" % rowsProcessed)
        f.close()

    else:

        # Use JDBC to upload data
        logger.info("Upload data using JDBC driver")

        # Init processed status file
        prname = dataPath+type+'/data/.processed'
        f = open(prname,"w+")
        f.write("{ \"processed\": %d , \"uploaded\":\"N\" }" % rowsProcessed)
        f.close()
   
    if path.exists(dtypeProFile) == True:
        os.remove(dtypeProFile)


