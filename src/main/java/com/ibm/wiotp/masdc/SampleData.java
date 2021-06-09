/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 * 
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.wiotp.masdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.*;
import java.util.Iterator;
import java.sql.*;
import java.sql.Types.*;
import java.nio.file.*;
import java.util.logging.*;
import java.util.Date;
import java.util.UUID;
import java.text.SimpleDateFormat;
import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;


// Extract data from SCADA historian database (mySQL and MSSQL), dump in a csv file,
// and execute script to process data

public class SampleData {

    private static final Logger logger = Logger.getLogger("sample-data");
    private static final String MASDC_VERSION = "1.0.1";

    /**
     * @param cofiguredConnectorType    Device or Alarm connector.
     *                                  Valid values are device or alarm. 
     *                                  This is a required parameter.
     */
    public static void main(String[] args) {

        Handler consoleHandler = null;
        String installDir = "";
        String dataDir = "";
        int tagMapVersion = 1;
        int testBreakMode = 0;

        try {
            // add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(){ 
                public void run() { 
                    System.out.println("Connector is shutting down."); 
                } 
            }); 

            if (args.length == 0 || args[0].isEmpty()) {
                logger.severe("Required argument connector type [device or alarm] is not specified.");
                System.exit(1);
            }

            String configuredConnectorType = args[0];

            // Get user home dir
            String userHome = System.getProperty("user.home");

            // Get install and data dir location from enviironment variables
            Map <String, String> map = System.getenv();
            for ( Map.Entry <String, String> entry: map.entrySet() ) {
                if ( entry.getKey().compareTo("IBM_DATAINGEST_INSTALL_DIR") == 0 ) {
                    installDir = entry.getValue();
                } else if ( entry.getKey().compareTo("IBM_DATAINGEST_DATA_DIR") == 0 ) {
                    dataDir = entry.getValue();
                }
            }
            if ( installDir.compareTo("") == 0 ) {
                installDir = userHome + "/ibm/masdc";
            } 
            if ( dataDir.compareTo("") == 0 ) {
                dataDir = userHome + "/ibm/masdc";
            } 

            // Set config
            Config config = new Config(installDir, dataDir, configuredConnectorType);
            config.set();

            // Setup logger
            String logFile = config.getLogFile();
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT : %4$s : %2$s : %5$s%6$s%n");
            logger.setUseParentHandlers(false);
            Handler[] handlers = logger.getHandlers();
            for ( int i = 0; i < handlers.length; i++ ) {
                System.out.printf("Handler %d\n", i);
                Handler lh = handlers[i];
                logger.removeHandler(lh);
            }
            FileHandler fh = new FileHandler(logFile, 5242880, 5, true);
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.removeHandler(consoleHandler);

            logger.info("");
            logger.info("==== MAS Connector  - Sample Data Extraction Utility");
            logger.info("Connector version: " + MASDC_VERSION);
            logger.info("Client Site: " + config.getClientSite());
            logger.info("Connector Type: " + configuredConnectorType);

            OffsetRecord offsetRecord = new OffsetRecord(config, false);
            String cacheName = config.getClientSite() + "_" + configuredConnectorType + "_tags_v" + tagMapVersion;
            CacheAccess<String, TagData> tagpaths = JCS.getInstance(cacheName);

            DBConnector dbconn = new DBConnector(config, offsetRecord, tagpaths);

            String tableName = "sqlt_data";
            String csvFilePath = dataDir + "/sqlt_data_2021_05_01.csv";
            dbconn.extractRawData(tableName, csvFilePath);

            tableName = "sqlth_te";
            csvFilePath = dataDir + "/sqlth_te.csv";
            dbconn.extractRawData(tableName, csvFilePath);

        } catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }

        logger.info("Shutting down MAS data connector.");
        System.exit(0);
    }

    private static int checkFileExists(String fileName) {
        int found = 0;
        try {
            File f = new File(fileName);
            if (f.exists()) {
                found = 1;
            }
        } catch (Exception e) {
            logger.info("File is not found. " + fileName + "  Msg: " + e.getMessage());
        }
        return found;
    }

}

