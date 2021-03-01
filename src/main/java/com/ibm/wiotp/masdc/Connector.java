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

public class Connector {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    /**
     * @param cofiguredConnectorType    Device or Alarm connector.
     *                                  Valid values are device or alarm. 
     *                                  This is a required parameter.
     */
    public static void main(String[] args) {

        Handler consoleHandler = null;
        String installDir = "";
        String dataDir = "";

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

            logger.info("MAS Data connector for Ignition SCADA historian.");
            logger.info("Client Site: " + config.getClientSite());
            logger.info("Connector Type: " + configuredConnectorType);
            logger.info("Monitor Table Name: IOT_" + config.getEntityType().toUpperCase());
            logger.info(String.format("Run mode: %d", config.getRunMode()));

            // Initialize required objects
            OffsetRecord offsetRecord = new OffsetRecord(config, false);
            String cacheName = config.getEntityType() + ".tags";
            CacheAccess<String, TagData> tagpaths = JCS.getInstance(cacheName);
            Set<String> tagList = tagpaths.getCacheControl().getKeySet();
            long totalDevices = tagList.size();
            logger.info(String.format("Total tags in cache: %d", totalDevices));
            offsetRecord.setTagCount(totalDevices);

            // Register device and entity type, and create monitoring tables
            try {
                DeviceType deviceType = new DeviceType(config);
                deviceType.register();
                EntityType entityType = new EntityType(config);
                entityType.register();
                MonitorTable monTable = new MonitorTable(config);
                monTable.create();
                // monTable.indexTable();
            } catch(Exception e) {
                logger.info("Exception thrown during creation of deviceType, entityType, or monitoring tables");
                logger.log(Level.INFO, e.getMessage(), e);
                System.exit(1);
            }

            // start data processing threads
            try {
                offsetRecord.setTagCount(tagpaths.getCacheControl().getSize());
                ConnectorStats connStats = new ConnectorStats(config, offsetRecord, tagpaths);
                connStats.start();
                Device device = new Device(config, tagpaths);
                device.start();
                Dimension dimension = new Dimension(config, tagpaths);
                dimension.start();
                StatsServer statsServer = new StatsServer(config, offsetRecord);
                statsServer.start();
                CLIServer server = new CLIServer(config, offsetRecord);
                server.start();
                // ProcessTagData shoule be the last thread to start
                ProcessTagData processTagData = new ProcessTagData(config, offsetRecord, tagpaths);
                processTagData.start();
            } catch(Exception e) {
                logger.severe("Processing threads failed to start. Check the last exception.");
            }

            if (config.getUpdateFlag() == 1) {
                logger.info("Update flag is set. Shutting down connector for update process to complete.");
            }
           
            JCS.shutdown();

        } catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }

        logger.info("Shutting down MAS data connector.");
        System.exit(0);
    }

}


