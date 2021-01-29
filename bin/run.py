#!/usr/bin/python3
#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
#
# *****************************************************************************
# Copyright (c) 2020 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
# Script to run device data extraction loop from SCADA historian and 
# Upload to IBM MAS Datalake.
#
# Extraction loop depends on existence of the following file
# IBM_DATAINGEST_DATA_DIR/volume/config/entity.dat
#
# This file should contain configuration file name for the entity type.
# For example CakebreadType.json
#

import os
import json
import pathlib
import logging
import sys
import time
from pathlib import Path
from os import path
import datetime
from datetime import date

userHome = str(Path.home())
defaultDir = userHome + "/ibm/masdc"
installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)
dibin = installDir + "/bin"
sys.path.append(dibin)
import utils

logger = logging.getLogger('dataingest')

#
# Main
#
if __name__ == "__main__":
    global rlfd

    # Command line options
    try:
        rtype = sys.argv[1]
    except IndexError:
        print("ERROR: Entity or alarm type is not specified")
        print("Usage: connector <entity | alarm | extract_sqlFileName>")
        exit(1)

    runtype='restart'

    # Create directories
    dirperm = 0o755
    retval = utils.createDir(dataDir, "volume", dirperm)
    retval += utils.createDir(dataDir, "volume/logs", dirperm)
    retval += utils.createDir(dataDir, "volume/config", dirperm)
    retval += utils.createDir(dataDir, "volume/data", dirperm)
    retval += utils.createDir(dataDir, "volume/data/csv", dirperm)

    # Open run type log file
    rtypeLogPath = dataDir + "/volume/logs/" + rtype + ".log"
    rfile = pathlib.Path(rtypeLogPath)
    if rfile.exists():
        rlfd = open(rtypeLogPath, "a+")
    else:
        rlfd = open(rtypeLogPath, "w+")
    rlfd.write("------------------------------------------- \n")
    rlfd.write("Start " + rtype + " processing.\n")
    now = datetime.datetime.now()
    dateStr = now.strftime("%m/%d/%Y, %H:%M:%S")
    rlfd.write("Date: " + dateStr + "\n")
    rlfd.flush()

    # Read run type configuration file
    entityDataFile = ''
    if rtype == 'alarm':
        entityDataFile = dataDir + "/volume/config/alarm.dat"
    elif rtype == 'entity':
        entityDataFile = dataDir + "/volume/config/entity.dat"
    else:
        # data extraction only
        CP = installDir + '/jre/lib/*:' + installDir + '/lib/*'
        if os.name == 'nt':
            CP = installDir + '/jre/lib/*;' + installDir + '/lib/*'
        logger.info("CP: %s", CP)
        command =  installDir + '/jre/bin/java -classpath "' + CP + '" com.ibm.wiotp.masdc.DBConnector extract ' + rtype
        logger.info("CMD: %s", command)
        logger.info("Start process to extract data: %s", rtype)
        os.system(command)
        exit()
    
    rlfd.write("Use data file: " + entityDataFile + "\n")
    foundFile = 0
    dtype = ""
    while foundFile == 0:
        file = pathlib.Path(entityDataFile)
        if file.exists():
            fp = open(entityDataFile, 'r')
            try:
                line = line = fp.readline()
                dtype = line.strip()
                foundFile = 1
            except:
                print("Invalid entity data file") 
                rlfd.write("Invalid entity data file. \n") 
                rlfd.flush()
            fp.close()
        else:
            runtype='start'
            rlfd.write("Waiting for input data file to get created in config directory. \n")
            rlfd.flush()
            time.sleep(15)

    if foundFile == 1:
        rlfd.write("Found input data file. \n")
        rlfd.flush()
        rlfd.close()

    # Create dtype log and data directories
    retval += utils.createDir(dataDir, "volume/logs/"+dtype, dirperm)
    retval += utils.createDir(dataDir, "volume/data/"+dtype, dirperm)
    dtypedir = "volume/data/" + dtype + "/data"
    retval += utils.createDir(dataDir, dtypedir, dirperm)

    # set log dir
    logdir = dataDir + "/volume/logs/"+dtype
    logfile = logdir + "/dataingest.log"
    if retval > 0:
        logdir = userHome
        logpath = userHome + "/dataingest.log"

    # change working directory to log dir
    os.chdir(logdir)

    # Set logger file handler and level
    hdlr = logging.handlers.RotatingFileHandler(logfile, mode='a', maxBytes=1048576, backupCount=5, encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s %(levelno)r: %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.DEBUG)

    logger.info("Start Data Ingest cycle")
    logger.info("----------------------------------") 
    logger.info(" MAS Data Connector ")
    logger.info("----------------------------------") 

    if retval > 0:
        logger.error("Failed to create required directories.")
        exit(1)

    print("Start processing cycle for entity " + dtype)
    logger.info("Start processing cycle for entity %s", dtype)

    # Read configuration file
    tableCfgPath = dataDir + "/volume/config/" + dtype + ".json"
    formatedSqlFilePath = dataDir + "/volume/data/" + dtype + "/data/" + dtype + ".sql"
    dataOffsetPath = dataDir + "/volume/config/" + dtype + ".offset"
    config = {}
    with open(tableCfgPath) as configFD:
        config = json.load(configFD)
        logger.info("Read entity type config items")
        configFD.close()

    # Get sql related config data
    startDate = ''
    dateFormat = ''
    dateChange = 0
    sqlStatement = ''
    dbconfig = config['database']
    if 'startDate' in dbconfig:
        startDate = dbconfig['startDate']
    sqlFile = dbconfig['sqlFile']
    sqlFilePath = dataDir + "/volume/config/" + sqlFile;
    with open(sqlFilePath, 'r') as f:
        sqlStatement = f.read()
        f.close()
    scanInterval = 60
    if 'scanInterval' in dbconfig:
        scanInterval = dbconfig['scanInterval']
    if scanInterval <= 0:
        scanInterval = 60
    formatSqlStatement = True
    if 'formatSqlStatement' in dbconfig:
        formatSqlStatement = dbconfig['formatSqlStatement']

    fCycleDone = 0
    nCycle = 0
    retval = 0
    while retval == 0:
        # Get last run record
        lastEndTS = 0
        if path.exists(dataOffsetPath) == True:
            with open(dataOffsetPath) as offsetFD:
                lrunCfg = json.load(offsetFD)
                offsetFD.close()
                if 'lastEndTS' in lrunCfg:
                    lastEndTS = lrunCfg['lastEndTS']
        else:
            # Create a new offset file
            recStr = '{"startDate":"' + startDate + '","startRow":0,"lastEndTS":0}'
            frs = open(dataOffsetPath, "w")
            frs.write(recStr)
            frs.close()

        sqlStatementFormatted = sqlStatement
        if formatSqlStatement == True:
            logger.info("Update SQL based on configured start date")
            nDay, nMonth, nYear, nCycle = utils.getNextExtractionDate(startDate, lastEndTS, nCycle)
            logger.info("Next Date data: %d %d %d" % (nYear, nMonth, nDay))
            nextDate = datetime.date(year=nYear,day=nDay,month=nMonth)
            startDate = datetime.date(year=nYear,day=nDay,month=nMonth).strftime('%Y-%m-%d') + " 00:00:00"
            logger.info("Start date: %s" % startDate)
            sqlStatementFormatted = nextDate.strftime(sqlStatement)

        if nCycle == 0 and fCycleDone > 0:
            # Reset offset file
            recStr = '{"startDate":"' + startDate + '","startRow":0,"lastEndTS":0}'
            frs = open(dataOffsetPath, "w")
            frs.write(recStr)
            frs.close()

        # write sqlStatement
        sqfd = open(formatedSqlFilePath, "w")
        sqfd.write(sqlStatementFormatted)
        sqfd.close()

        dtypeProFile = dataDir + "/volume/config/" + dtype + ".running"
        if runtype == 'start':
            if path.exists(dtypeProFile) == True:
                print("Entity " + dtype + " processing is in locked state")
                print("If you want to restart, stop this process and use restart option, to unlock and start")
                time.sleep(scanInterval)
                continue

        print("Start next cycle ...")
        logger.info("Start next cycle ...")
        f = open(dtypeProFile,"w+")
        f.write("{ \"started\": %s }" % dtype)
        f.close()
           
        utils.createDir(dataDir, "volume/data/" + dtype, dirperm)
        utils.createDir(dataDir, "volume/data/" + dtype + "/schemas", dirperm)
        utils.createDir(dataDir, "volume/data/" + dtype + "/data", dirperm)

        CP = installDir + '/jre/lib/*:' + installDir + '/lib/*'
        if os.name == 'nt':
            CP = installDir + '/jre/lib/*;' + installDir + '/lib/*'
        logger.info("CP: %s", CP)
        command =  installDir + '/jre/bin/java -classpath "' + CP + '" com.ibm.wiotp.masdc.DBConnector ' + dtype
        logger.info("CMD: %s", command)
        logger.info("Start process for dtype: %s", dtype)
        os.system(command)
        logger.info("Wait for next scan to start in %d seconds", scanInterval)
        nCycle += 1
        fCycleDone = 1
        time.sleep(scanInterval)

