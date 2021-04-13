/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.sql.*;
import java.sql.Types.*;
import org.apache.commons.jcs3.access.CacheAccess;

public class ConnectorStats {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static OffsetRecord offsetRecord = null;
    private static Config config = null;
    private static DBHelper dbHelper = null;
    private static DBConnector dbConnector = null;
    private static int connectorType;
    private static String entityType;
    private static String checkUpdateFile;

    public ConnectorStats(Config config, OffsetRecord offsetRecord) throws Exception {
        if (config == null || offsetRecord == null) {
            throw new NullPointerException("config/offsetRecord parameter cannot be null");
        }

        this.config = config;
        this.offsetRecord = offsetRecord;
        this.dbHelper = new DBHelper(config);

        connectorType = config.getConnectorType();
        entityType = config.getEntityType();
        checkUpdateFile = config.getUpdateFile();
    }

    public void start() throws Exception {
        try {
            dbConnector = new DBConnector(config, offsetRecord, null);
            startConnectorStatsThread();
        } catch(Exception e) {
            logger.log(Level.INFO, e.getMessage(), e);
            throw e;
        }
    }

    private static void startConnectorStatsThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Heartbeat thread: send status");
                    // check for .update file in config directory
                    File f = new File(checkUpdateFile);
                    if (f.exists()) {
                        config.setUpdateFlag(1);
                        break;
                    } else {                        
                        config.setUpdateFlag(0);
                    } 
                    dbConnector.insertStats(offsetRecord);
                    try {
                        Thread.sleep(60000);
                    } catch (Exception e) {}
                }
            }
        };
        logger.info("Starting Connector Stats and heartbeat thread ...");
        new Thread(thread).start();
        logger.info("Connector Stats and heartbeat thread is started.");
        return;
    }

}


