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
# Script to run alarm data extraction loop from SCADA historian and 
# Upload to IBM MAS Datalake.
#
# Extraction loop depends on existence of the following file
# IBM_DATAINGEST_DATA_DIR/volume/config/alarm.dat
#
# This file should contain configuration file name for the entity type.
# For example CakebreadAlarmType.json
#

import os
import json
from pathlib import Path
import logging
import sys
import time
from os import path
from datetime import date

logger = logging.getLogger('dataingest')

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


#
# Main
#
if __name__ == "__main__":

    runtype='restart'
    userHome = str(Path.home())
    defaultDir = userHome + "/ibm/masdc"
    installDir = os.getenv('IBM_DATAINGEST_INSTALL_DIR', defaultDir)
    dataDir = os.getenv('IBM_DATAINGEST_DATA_DIR', defaultDir)

    # entity config file
    alarmDataFile = dataDir + "/volume/config/alarm.dat"
    foundFile = 0
    atype = ""
    while foundFile == 0:
        file = pathlib.Path(alarmDataFile)
        if file.exists():
            fp = open(alarmDataFile, 'r')
            try:
                line = line = fp.readline()
                atype = line.strip()
                foundFile = 1
            except:
                print("Invalid alarm data file") 
            fp.close()
        else:
            runtype='start'
            time.sleep(15)


    # Create log directories
    dirperm = 0o755
    retval = createDir(dataDir, "volume", dirperm)
    retval += createDir(dataDir, "volume/logs", dirperm)
    retval += createDir(dataDir, "volume/logs/"+atype, dirperm)

    # set log dir
    logdir = dataDir + "/volume/logs/"+atype
    logfile = logdir + "/dataingest.log"
    if retval > 0:
        logdir = userHome
        logpath = userHome + "/dataingest.log"

    # change working directory to log dir
    os.chdir(logdir)

    # Set logger file handler and level
    hdlr = logging.FileHandler(logfile, mode='a', encoding=None, delay=False)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s %(levelno)r: %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.DEBUG)

    logger.info("Start Data Ingest cycle")
    logger.info("----------------------------------") 
    logger.info(" WIoTP Bulk Data Upload Connector ")
    logger.info("----------------------------------") 

    # Create config and data directories
    retval = createDir(dataDir, "volume/data", dirperm)
    retval += createDir(dataDir, "volume/data/csv", dirperm)
    retval += createDir(dataDir, "volume/config", dirperm)

    if retval > 0:
        logger.error("Failed to create data directories.")
        exit(1)

    print("Start processing cycle for entity " + atype)
    logger.info("Start processing cycle for entity %s", atype)

    # Get scanInterval from atype configuration file
    tableCfgPath = dataDir + "/volume/config/" + atype + ".json"
    config = {}
    with open(tableCfgPath) as configFD:
        config = json.load(configFD)
        logger.info("Read entity type config items")
        configFD.close()

    scanInterval = 120
    tableNameChangeDate = 0
    sqlStatementFormat = ''
    dbconfig = config['database']
    if 'tableNameChangeDate' in dbconfig:
        tableNameChangeDate = dbconfig['tableNameChangeDate']
        if tableNameChangeDate < 0 or tableNameChangeDate > 31:
            tableNameChangeDate = 0
    if 'sqlStatementFormat' in dbconfig:
        sqlStatementFormat = dbconfig['sqlStatementFormat']
    if 'scanInterval' in dbconfig:
        scanInterval = dbconfig['scanInterval']
    if scanInterval <= 0:
        scanInterval = 120

    retval = 0
    while retval == 0:
        today = date.today()
        if tableNameChangeDate != 0:
            dbconfig['sqlStatement'] = today.strftime(sqlStatementFormat)
            configFD = open(tableCfgPath, "r+")
            configFD.write(json.dumps(config, indent=4))
            configFD.close()

        atypeProFile = dataDir + "/volume/config/" + atype + ".running"
        if runtype == 'start':
            if path.exists(atypeProFile) == True:
                print("Entity " + atype + " processing is in locked state")
                print("If you want to restart, stop this process and use restart option, to unlock and start")
                time.sleep(scanInterval)
                continue

        print("Start next cycle ...")
        logger.info("Start next cycle ...")
        f = open(atypeProFile,"w+")
        f.write("{ \"started\": %s }" % atype)
        f.close()
           
        retval = createDir(dataDir, "volume/data/" + atype, dirperm)
        retval += createDir(dataDir, "volume/data/" + atype + "/schemas", dirperm)
        retval += createDir(dataDir, "volume/data/" + atype + "/data", dirperm)
        CP = installDir + '/jre/lib/*:' + installDir + '/lib/*'
        if os.name == 'nt':
            CP = installDir + '/jre/lib/*;' + installDir + '/lib/*'
        logger.info("CP: %s", CP)
        command =  installDir + '/jre/bin/java -classpath "' + CP + '" com.ibm.wiotp.masdc.DBConnector ' + atype + ' register'
        logger.info("CMD: %s", command)
        logger.info("Start process for atype: %s", atype)
        os.system(command)

        time.sleep(scanInterval)

