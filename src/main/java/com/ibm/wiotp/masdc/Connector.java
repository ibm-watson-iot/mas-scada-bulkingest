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
import java.util.List;
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

    static String logFile = "";
    static int batchInsertSize = 10000;
    static String csvFilePath = "";
    static String tableName = "";
    static Config config;
    static int connectorType = 1;
    static String cType;
    static int runMode = 0;
    static String client;
    static JSONObject wiotp;
    static JSONObject datalake;
    static JSONObject scada;
    static OffsetRecord offsetRecord;
    static int masHBStats = 1;
    static String[] masHBDevices = { "masExtract", "masUpload", "masRate"};
    static String[] masHBStatNames = {"extracted", "uploaded", "rate"};
    static int[] masHBTagIds = {-9001, -9002, -9003};
    static CacheAccess<String, TagData> tagpaths = null;
    static String cacheName = null;
    static int totalTagCount = 0;


    /**
     * @param cType    Device or Alarm connector.
     *                 Valid values are device or alarm. 
     *                 This is a required parameter.
     */
    public static void main(String[] args) {

        Handler consoleHandler = null;

        String installDir = "";
        String dataDir = "";
        String extractSqlFile = "";

        try {
            // consoleHandler = new ConsoleHandler();
            // logger.addHandler(consoleHandler);

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

            cType = args[0];

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
            config = new Config(installDir, dataDir, cType);
            config.set();
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

            // Get config objects
            tableName = config.getEntityType();
            connectorType = config.getConnectorType();
            client = config.getClientSite();
            runMode = config.getRunMode();
            scada = config.getIgnitionConfig();
            datalake = config.getMonitorConfig();
            wiotp = config.getWiotpConfig();

            logger.info("MAS Data connector for Ignition SCADA historian.");
            logger.info("Client Site: " + client);
            logger.info("Connector Type: " + cType);
            logger.info("Monitor Table Name: IOT_" + tableName.toUpperCase());
            logger.info(String.format("Run mode: %d", runMode));

            // init objects and set required file paths
            offsetRecord = new OffsetRecord(config, false);
            cacheName = tableName + ".tags";
            // String cacheConfig = dataDir + "/volume/config/tagCache.ccf";
            // Properties props = new Properties(); 
            // props.load(new FileInputStream(cacheConfig)); 
            // JCS.setConfigProperties(props); 
            tagpaths = JCS.getInstance(cacheName);
            csvFilePath = dataDir + "/volume/data/" + tableName + ".csv";

            // Create device type
            DeviceType deviceType = new DeviceType(config, client, cType, tableName, wiotp);
            deviceType.apply();

            // create entity type
            EntityType entityType = new EntityType(wiotp.getString("geo"), wiotp.getString("tenantId"), cType, tableName, datalake.getString("schema"));
            entityType.build();
            RestClient restClient = new RestClient(entityType.getBaseUrl(), 2, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
            restClient.post(entityType.getMethod(), entityType.toString());
            logger.info(String.format("EntityType POST Status Code: %d", restClient.getResponseCode()));

            // Create Monitoring Table
            MonitorTable monTable = new MonitorTable(cType, tableName, datalake);
            monTable.build();
            monTable.create();
            // monTable.indexTable();

            // Add HeartBeat tags in tag cache
            for (int i=0; i < Constants.HB_TAGS; i++) {
                String masHbTag = masHBDevices[i] + "/" + masHBStatNames[i];
                TagData td = new TagData(masHbTag, masHBDevices[i]);
                tagpaths.put(masHbTag, td);
                totalTagCount += 1;
            }

            // start data processing threads
            startHearbeatThread();
            startDeviceThread();
            startDimensionThread();

            // Start extract data, and upload loop
            extractAndUpload();

            JCS.shutdown();

        } catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }

        if (runMode == Constants.TEST) System.exit(0);
    }


    // Extract and Upload data to data lake
    private static void extractAndUpload() {

        FileWriter fw = null;
        Connection conn = null;
        Statement stmt = null;
        int noUpload = 1;

        try {
            logger.info("Connecting to source to extract data for " + tableName);

            int type = config.getIgnitionDBType();
            String DB_URL = config.getIgnitionDBUrl();
            String[] dbCols = config.getMonitorDBCols();
      
            // retrieve records
            long cycleStartTimeMillis = 0;
            long cycleEndTimeMillis = 0;
            long cycleTime = 0;
            List<String> sourceDBColumnNames = new ArrayList<String>();
            int columnCount = 0;

            while ( true ) {

                cycleStartTimeMillis = System.currentTimeMillis();

                long startTimeSecs = offsetRecord.getStartTimeSecs();
                long startTimeMilli = startTimeSecs * 1000;
                long endTimeSecs = offsetRecord.getEndTimeSecs();
                long endTimeMilli = endTimeSecs * 1000;
                int month = offsetRecord.getMonth();
                int year = offsetRecord.getYear();

                logger.info(String.format("StartTime:%d EndTime:%d Year:%d Month:%d currTime:%d", 
                    startTimeSecs, endTimeSecs, year, month, (cycleStartTimeMillis/1000)));

                String querySql = config.getIgnitionDBSql(startTimeMilli, endTimeMilli, year, month);
                if (runMode != Constants.PRODUCTION) {
                    logger.info("Extract SQL: " + querySql);
                }

                if ( type == 1 ) {
                    conn = DriverManager.getConnection(DB_URL, config.getIgnitionDBUser(), config.getIgnitionDBPass());
                } else {
                    conn = DriverManager.getConnection(DB_URL);
                }

                stmt = conn.createStatement();

                ResultSet rs = null;
                int gotData = 1;
                try {
                    rs = stmt.executeQuery(querySql);
                } catch (Exception qex) {
                    if (qex instanceof SQLException) {
                        int errCode = ((SQLException)qex).getErrorCode();
                        logger.info("Extract: SQLException error code: " + errCode);

                        if (errCode == 1146) {
                            /* mySQL - table doesn't exist - */
                            offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, offsetRecord.STATUS_NO_TABLE);
                            gotData = 0;
                        } else {
                            throw qex;
                        }
                    } else {
                        throw qex;
                    }
                }

                if (gotData == 0) {
                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {}
                    continue;
                }

                // Get column count and column type of TS column and cache it
                columnCount = sourceDBColumnNames.size();
                if (columnCount == 0) {
                    sourceDBColumnNames = new ArrayList<String>();
                    final ResultSetMetaData rsmd = rs.getMetaData();
                    columnCount = rsmd.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = rsmd.getColumnName(i);
                        sourceDBColumnNames.add(colName);
                    }
                }

                Map<String, List<Object>> sourceMap = new HashMap<String, List<Object>>();
                for (int i=0; i < dbCols.length; i++) {
                    sourceMap.put(dbCols[i], new ArrayList<>());
                }

                if (runMode != Constants.PRODUCTION && columnCount > 0) {
                    fw = new FileWriter(csvFilePath);
                    int j = 1;
                    for (String colName : sourceDBColumnNames) {
                        fw.append(colName);
                        if ( j < columnCount ) fw.append(",");
                        j += 1;
                    }
                    fw.append(System.getProperty("line.separator"));
                }

                long rowCount = 0;
                long lastTimeStamp = 0;
                while (rs.next()) {
                    sourceMap.get("DEVICETYPE").add(tableName);
                    sourceMap.get("EVENTTYPE").add(cType);
                    sourceMap.get("FORMAT").add("JSON");
                    sourceMap.get("LOGICALINTERFACE_ID").add("null");

                    for (int i = 1; i <= columnCount; i++) {
                        String colName = sourceDBColumnNames.get(i-1);
                        if (rs.getObject(i) != null) {
                            if (colName.equals("tagpath")) {
                                String tagpath = rs.getString(i);
                                String data = UUID.nameUUIDFromBytes(tagpath.getBytes()).toString();
                                TagData td = new TagData(tagpath, data);
                                try {
                                    tagpaths.putSafe(tagpath, td);
                                    totalTagCount += 1;
                                } catch(Exception e) {}
                                sourceMap.get("DEVICEID").add(data);
                                String[] tagelems = tagpath.split("/");
                                if (connectorType == 1) {
                                    sourceMap.get("EVT_NAME").add(tagelems[tagelems.length-1]);
                                }
                                if (fw != null) fw.append(data);
                            } else if (colName.equals("t_stamp")) {
                                lastTimeStamp = rs.getLong(i);
                                Timestamp ts = new Timestamp(lastTimeStamp);
                                sourceMap.get("RCV_TIMESTAMP_UTC").add(ts);
                                sourceMap.get("UPDATED_UTC").add(ts);
                                if (fw != null) fw.append(ts.toString());
                            } else if (colName.equals("eventtype")) {
                                sourceMap.get("ETYPE").add(rs.getObject(i));
                                if (fw != null) fw.append(rs.getString(i));
                            } else if (colName.equals("eventtime")) {
                                sourceMap.get("RCV_TIMESTAMP_UTC").add(rs.getObject(i));
                                sourceMap.get("UPDATED_UTC").add(rs.getObject(i));
                                if (fw != null) fw.append(rs.getString(i));
                            } else if (colName.equals("id")) {
                                sourceMap.get("ALARMID").add(rs.getLong(i));
                                if (fw != null) fw.append(rs.getString(i).replaceAll(",", " "));
                            } else { 
                                sourceMap.get(colName.toUpperCase()).add(rs.getObject(i));
                                if (fw != null) fw.append(rs.getString(i).replaceAll(",", " "));
                            }
                        } else {
                            String data = "null";
                            sourceMap.get(colName.toUpperCase()).add(data);
                            if (fw != null) fw.append(data);
                        }
                        if ( i < columnCount && fw != null) fw.append(",");
                    }
                    if (fw != null) fw.append(System.getProperty("line.separator"));
                    rowCount += 1;
                }
                if (fw != null) {
                    fw.flush();
                    fw.close();
                }
                rs.close();
                conn.close();

                long currentTotalCount = offsetRecord.setProcessedCount(rowCount);

                if (rowCount == 0) {
                    sourceMap.clear();
                    int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, offsetRecord.STATUS_TABLE_NO_DATA);
                    long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
                    logger.info(String.format("No Data extracted: currCount=%d waitTimeMilli=%d currTagCount=%d\n", 
                        currentTotalCount, waitTime, totalTagCount));
                    try {
                        Thread.sleep(waitTime);
                    } catch (Exception e) {}
                    continue;
                }

                logger.info(String.format("Data extracted: cols=%d rows=%d currCount=%d currTagCount=%d\n", 
                    columnCount, rowCount, currentTotalCount, totalTagCount));

                int nuploaded = 0;
                if (rowCount > 0) {
                    nuploaded = batchInsert(sourceMap, rowCount);
                    offsetRecord.setUploadedCount(nuploaded);
                }

                sourceMap.clear();

                int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, offsetRecord.STATUS_TABLE_WITH_DATA);
                long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);

                // calculate record processing rate
                cycleEndTimeMillis = System.currentTimeMillis();
                long timeDiff = (cycleEndTimeMillis - cycleStartTimeMillis);
                long rate = rowCount * 1000 / timeDiff;
                offsetRecord.setRate(rate);

                logger.info(String.format("Cycle stats: extracted:%d uploaded=%d rate=%d waitTimeMilli=%d currTagCount=%d\n", 
                        rowCount, nuploaded, rate, waitTime, totalTagCount));

                if (runMode == Constants.TEST) {
                   break;
                }

                try {
                    Thread.sleep(waitTime);
                } catch (Exception e) {}
            }
        
            logger.info("Data processing cycle is complete.");
					
        } catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }
    }

    private static int batchInsert(Map<String, List<Object>> sourceMap, long totalRows) {
        Connection con;
        Statement stmt;
        ResultSet rs;
        int rowsProcessed = 0;
        int batchCount = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            String monitorConnStr = config.getMonitorDBUrl();
            String insertSQL = config.getMonitorInsertSQL();
            logger.info("SQL Stmt: " + insertSQL);

            con = DriverManager.getConnection(monitorConnStr, config.getMonitorDBUser(), config.getMonitorDBPass());
            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement(insertSQL);

            for (int i=0; i<totalRows; i++) {
                try {
                    ps = config.getMonitorPS(ps, sourceMap, i);
                    ps.addBatch();
                    batchCount += 1;
                    rowsProcessed += 1; 
                    if ( batchCount >= batchInsertSize ) {
                        String msg = String.format("Batch update table: count:%d", batchCount);
                        logger.info(msg);
                        ps.executeBatch();
                        con.commit();
                        ps.clearBatch();
                        batchCount = 0;
                    }
                } catch (Exception ex) {
                    logger.log(Level.INFO, ex.getMessage(), ex);
                    break;
                }
            }
            if ( batchCount > 0 ) {
                String msg = String.format("Batch update table. count:%d", batchCount);
                logger.info(msg);
                ps.executeBatch();
                con.commit();
                ps.clearBatch();
            }
            con.commit();
            con.close();
            String msg = String.format("Total rows processed: %d", rowsProcessed);
            logger.info(msg);
        }
        catch (Exception e) {
            logger.info("rowsProcessed: " + rowsProcessed + " batchCount: " + batchCount);
            logger.log(Level.INFO, e.getMessage(), e);
            if (e instanceof SQLException) {
                // if (e.getMessage().contains("getNextException")) {
                    SQLException ne = ((SQLException)e).getNextException();
                    logger.log(Level.INFO, ne.getMessage(), ne);
                // }
            } 
        }

        return rowsProcessed;
    }

    private static void sendHeartbeatEvent() {
        long[] statValues = {offsetRecord.getProcessedCount(), offsetRecord.getUploadedCount(), offsetRecord.getRate()};

        // Add Connector status event - used as heart beat
        String[] dbCols = config.getMonitorDBCols();
        Map<String, List<Object>> statusMap = new HashMap<String, List<Object>>();
        for (int i=0; i < dbCols.length; i++) {
            statusMap.put(dbCols[i], new ArrayList<>());
        }

        // add stats
        int tagid = 0;
        int value = 1;
        int intval = 0;
        double dval = 0.0;
        String strval = "null";
        long statValue;
        String statName;
        for (int i=0; i < Constants.HB_TAGS; i++) {
            if (connectorType == 1) {
                statusMap.get("TAGID").add(masHBTagIds[i]);
                statusMap.get("INTVALUE").add(statValues[i]);
                statusMap.get("FLOATVALUE").add(dval);
                statusMap.get("STRINGVALUE").add(strval);
                statusMap.get("DATEVALUE").add(strval);
                statusMap.get("EVT_NAME").add(masHBStatNames[i]);
                statusMap.get("DEVICETYPE").add(tableName);
                statusMap.get("DEVICEID").add(masHBDevices[i]);
                statusMap.get("LOGICALINTERFACE_ID").add("null");
                statusMap.get("EVENTTYPE").add("status");
                statusMap.get("FORMAT").add("JSON");
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                statusMap.get("RCV_TIMESTAMP_UTC").add(ts);
                statusMap.get("UPDATED_UTC").add(ts);
            } else {
                statusMap.get("ALARMID").add(masHBTagIds[i]);
                statusMap.get("EVENTID").add(strval);
                statusMap.get("ACKBY").add(strval);
                statusMap.get("NAME").add(strval);
                statusMap.get("ETYPE").add(dval);
                statusMap.get("DISPLAYPATH").add(masHBStatNames[i]);
                statusMap.get("PRIORITY").add(dval);
                statusMap.get("VALUE").add(statValues[i]);
                statusMap.get("DEVICETYPE").add(tableName);
                statusMap.get("DEVICEID").add(masHBDevices[i]);
                statusMap.get("LOGICALINTERFACE_ID").add("null");
                statusMap.get("EVENTTYPE").add("status");
                statusMap.get("FORMAT").add("JSON");
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                statusMap.get("RCV_TIMESTAMP_UTC").add(ts);
                statusMap.get("UPDATED_UTC").add(ts);
            }
        }
        batchInsert(statusMap, Constants.HB_TAGS);
        statusMap.clear();
    }

    // Heartbeat thread - start this thread only after monitor tables are created
    private static void startHearbeatThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Heartbeat thread: send status");
                    sendHeartbeatEvent();
                    try {
                        Thread.sleep(60000);
                    } catch (Exception e) {}
                }
            }        
        };
        logger.info("Starting Heartbeat thread ......");
        new Thread(thread).start();
        logger.info("Heartbeat thread is started");
        return;
    }    

    // Thread to create devices 
    private static void startDeviceThread() {
        Runnable thread = new Runnable() {
            public void run() {
                Device device = new Device(config, client, cType, tableName, wiotp);
                while(true) {
                    logger.info("Start add devices cycle");
                    device.apply(tagpaths);
                    try {
                        Thread.sleep(10000);
                    } catch (Exception e) {}
                }
            }        
        };
        logger.info("Starting AddDevice thread ......");
        new Thread(thread).start();
        logger.info("AddDevice thread is started");
        return;
    }    


    // Thread to create dimensions data
    private static void startDimensionThread() {
        Runnable thread = new Runnable() {
            public void run() {
                Dimension dim = new Dimension(config, client, cType, tableName, wiotp);
                while(true) {
                    logger.info("Start add dimension data cycle");
                    dim.apply(tagpaths);
                    try {
                        Thread.sleep(10000);
                    } catch (Exception e) {}
                }
            }        
        };
        logger.info("Starting AddDimension thread ......");
        new Thread(thread).start();
        logger.info("AddDimension thread is started");
        return;
    }    

}


