/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.io.FileWriter;
import java.sql.*;
import java.sql.Types.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.ArrayList;
import java.util.UUID;
import java.text.DecimalFormat;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.jcs3.access.CacheAccess;


public class DBConnector {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config = null;
    private static DBHelper dbHelper = null;
    private static OffsetRecord offsetRecord = null;
    private static int type;
    private static String DB_URL;
    private static String[] dbCols;
    private static String entityType;
    private static List<String> entityTypes;
    private static String statsDeviceType;
    private static String statsDeviceId;
    private static int runMode;
    private static String csvFile;
    private static int connectorType;
    private static String connectorTypeStr;
    private static CacheAccess<String, TagData> tagpaths;
    private static int batchInsertSize = 10000;
    private static int sourceDBConnState = 0;
    private static int destDBConnState = 0;
    private static int sourceDBColumnCount = 0;
    private static String clientSite;
    private static int dataPoints;
    private static Device connectorStatDevice;

    public DBConnector(Config config, OffsetRecord offsetRecord, CacheAccess<String, TagData> tagpaths) throws Exception {
        if (config == null || offsetRecord == null) {
            throw new NullPointerException("config/offsetRecord parameter cannot be null");
        }

        this.config = config;
        this.offsetRecord = offsetRecord;
        this.tagpaths = tagpaths;

        clientSite = config.getClientSite();
        type = config.getIgnitionDBType();
        entityType = config.getEntityType();
        entityTypes = config.getTypes();
        runMode = config.getRunMode();
        csvFile = config.getCSVFile();
        connectorType = config.getConnectorType();
        connectorTypeStr = config.getConnectorTypeStr();
        batchInsertSize = config.getBatchInsertSize();
        statsDeviceType = config.getStatsDeviceType();
        statsDeviceId = config.getStatsDeviceId();
        dataPoints = config.getDataPoints();

        this.dbHelper = new DBHelper(config);
        dbCols = dbHelper.getMonitorDBCols();
        DB_URL = config.getIgnitionDBUrl();

        connectorStatDevice = new Device(config, tagpaths);
    }    


    // Extract and Upload data to data lake
    public static void extractAndUpload() throws Exception {

        FileWriter fw = null;
        Connection conn = null;
        Statement stmt = null;
        int noUpload = 1;
        ExecutorService uploadWorkerPool = Executors.newCachedThreadPool();;

        try {
            logger.info("Connecting to source to extract data for " + entityType);

            // retrieve records
            long cycleStartTimeMillis = 0;
            long cycleEndTimeMillis = 0;
            long cycleTime = 0;
            List<String> sourceDBColumnNames = new ArrayList<String>();

            while ( true ) {

                // Check connector stop flag, if set wait fo 60 seconds and check again
                JSONObject statDeviceMetadata = connectorStatDevice.getDeviceMetataData(statsDeviceType, statsDeviceId);
                int stopFlag = statDeviceMetadata.optInt("stopFlag", 0);
                if (stopFlag == 1) {
                    // System.out.println("Connector stop flag is set. Wait for 60 seconds and then check again.");
                    logger.info("Connector stop flag is set. Wait for 60 seconds and then check again.");
                    Thread.sleep(60000);
                    continue;
                }

                // check update flag is set wait for 10 seconds and return
                if (config.getUpdateFlag() == 1) {
                    logger.info("Update flag is set. Wait for 10 seconds for process to be stopped.");
                    Thread.sleep(10000);
                    break;
                }

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

                conn = getSourceConnection(type);
                stmt = conn.createStatement();

                ResultSet rs = null;
                int gotData = 1;
                try {
                    rs = stmt.executeQuery(querySql);
                } catch (Exception qex) {
                    gotData = 0;
                    if (qex instanceof SQLException) {
                        int errCode = ((SQLException)qex).getErrorCode();
                        if (errCode == 1146) {
                            // mySQL - table doesn't exist. Set offset file to move to next month table
                            offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_NO_TABLE);
                        } else {
                            logger.info("Extract: SQLException: " + qex.getMessage());
                        }
                    } else {
                        logger.info("Extract: Exception: " + qex.getMessage());
                    }
                }

                if (gotData == 0) {
                    resetDBConnection(stmt, rs, conn);
                    continue;
                }

                // Get column count and column type of TS column and cache it
                try {
                    sourceDBColumnCount = sourceDBColumnNames.size();
                    if (sourceDBColumnCount == 0) {
                        sourceDBColumnNames = new ArrayList<String>();
                        final ResultSetMetaData rsmd = rs.getMetaData();
                        sourceDBColumnCount = rsmd.getColumnCount();
                        for (int i = 1; i <= sourceDBColumnCount; i++) {
                            String colName = rsmd.getColumnName(i);
                            sourceDBColumnNames.add(colName);
                        }
                    }
                } catch(Exception e) {
                    resetDBConnection(stmt, rs, conn);
                    continue;
                }
   
                // Prepare extracted data for upload: create hash map of extracted data 
                long rowCount = 0;
                long currentTotalCount = 0;
                Map<String, List<Object>> sourceMap = new HashMap<String, List<Object>>();
                try {
                    rowCount = getSourceMap(sourceDBColumnNames, rs, sourceMap);
                    currentTotalCount = offsetRecord.setProcessedCount(rowCount);
                } catch(Exception e) {
                    logger.log(Level.FINE, e.getMessage(), e);
                } 
  
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
 
                if (rowCount == 0) {
                    if (sourceMap != null) sourceMap.clear();
                    int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_NO_DATA);
                    long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
                    logger.info(String.format("No Data extracted: currCount=%d waitTimeMilli=%d entities=%d\n", 
                        currentTotalCount, waitTime, offsetRecord.getEntityCount()));
                    try {
                        Thread.sleep(waitTime);
                    } catch (Exception e) {}
                    continue;
                }
    
                logger.info(String.format("Data extracted: cols=%d rows=%d currCount=%d entities=%d\n", 
                    sourceDBColumnCount, rowCount, currentTotalCount, offsetRecord.getEntityCount()));


                // Upload data
                int nuploaded = 0;
                if (rowCount > 0) {
                    ListIterator<String> itr = null;
                    itr = entityTypes.listIterator();
                    while (itr.hasNext()) {
                        String eType = itr.next();
                        nuploaded = batchInsert(sourceMap, rowCount, eType);
                        offsetRecord.setUploadedCount(nuploaded);
                    }
                }

                sourceMap.clear();

                int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
                long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);

                // calculate record processing rate
                cycleEndTimeMillis = System.currentTimeMillis();
                long timeDiff = (cycleEndTimeMillis - cycleStartTimeMillis);
                long rate = rowCount * 1000 / timeDiff;
                offsetRecord.setRate(rate);

                logger.info(String.format("Cycle stats: extracted:%d rate=%d waitTimeMilli=%d currEntityCount=%d\n", 
                        rowCount, rate, waitTime, offsetRecord.getEntityCount()));
                if (runMode == Constants.TEST) {
                   break;
                }

                try {
                    Thread.sleep(waitTime);
                } catch (Exception e) {}
            }
        
            logger.info("Data processing cycle is complete.");
					
        } catch (Exception ex) {
            throw ex;
        }
    }

    public static int batchInsert(Map<String, List<Object>> sourceMap, long totalRows, String eType) {
        int rowsProcessed = 0;
        int batchCount = 0;
        String connStr = config.getMonitorDBUrl();
        String dbUser = config.getMonitorDBUser();
        String dbPassword = config.getMonitorDBPass();
       
        if (connStr == null || dbUser == null || dbPassword == null) {
            logger.severe("Monitor DB Configuration issue: null URL, user or password");
            return rowsProcessed;
        } 
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");

            String insertSQL = dbHelper.getMonitorInsertSQL(eType);
            logger.info("SQL Stmt: " + insertSQL);

            Connection conn = getDestinationDBConnection(connStr, dbUser, dbPassword);
            PreparedStatement ps = conn.prepareStatement(insertSQL);

            for (int i=0; i<totalRows; i++) {
                try {
                    String sourceMapEType = String.valueOf(sourceMap.get("DEVICETYPE").get(i));
                    if (sourceMapEType.equals(eType)) {
                        ps = dbHelper.getMonitorPS(ps, sourceMap, i);
                        ps.addBatch();
                        batchCount += 1;
                        rowsProcessed += 1; 
                        if ( batchCount >= batchInsertSize ) {
                            logger.info(String.format("Batch update table: count:%d", batchCount));
                            try {
                                ps.executeBatch();
                            } catch(Exception bex) {
                                logger.log(Level.FINE, bex.getMessage(), bex);
                                rowsProcessed = rowsProcessed - batchCount; 
                                if (bex instanceof SQLException) {
                                    SQLException ne = ((SQLException)bex).getNextException();
                                    logger.log(Level.FINE, ne.getMessage(), ne);
                                }
                            }
                            conn.commit();
                            ps.clearBatch();
                            batchCount = 0;
                        }
                    }
                } catch (Exception ex) {
                    logger.log(Level.FINE, ex.getMessage(), ex);
                    break;
                }
            }
            if ( batchCount > 0 ) {
                logger.info(String.format("Batch update table. count:%d", batchCount));
                try {
                    ps.executeBatch();
                } catch(Exception bex) {
                    logger.log(Level.FINE, bex.getMessage(), bex);
                    rowsProcessed = rowsProcessed - batchCount; 
                    if (bex instanceof SQLException) {
                        SQLException ne = ((SQLException)bex).getNextException();
                        logger.log(Level.FINE, ne.getMessage(), ne);
                    }
                }
                conn.commit();
                ps.clearBatch();
            }
            conn.commit();
            conn.close();
            logger.info(String.format("Total rows processed: %d", rowsProcessed));
        }
        catch (Exception e) {
            logger.info("rowsProcessed: " + rowsProcessed + " batchCount: " + batchCount);
        }

        return rowsProcessed;
    }

    public static int insertStats(OffsetRecord offsetRecord) {
        int rowsProcessed = 1;
        String connStr = config.getMonitorDBUrl();
        String dbUser = config.getMonitorDBUser();
        String dbPassword = config.getMonitorDBPass();
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");

            String insertSQL = dbHelper.getConectorStatsInsertSQL(config.getStatsDeviceType());
            logger.info("SQL Stmt: " + insertSQL);

            Connection conn = getDestinationDBConnection(connStr, dbUser, dbPassword);
            PreparedStatement ps = conn.prepareStatement(insertSQL);

            ps.setLong(1, offsetRecord.getProcessedCount());
            ps.setDouble(2, offsetRecord.getUploadedCount());
            ps.setDouble(3, offsetRecord.getRate());
            ps.setDouble(4, offsetRecord.getEntityTypeCount());
            ps.setDouble(5, offsetRecord.getEntityCount());
           
            Timestamp ts = new Timestamp(offsetRecord.getStartTimeSecs() * 1000);
            ps.setString(6, ts.toString());
            ts = new Timestamp(offsetRecord.getEndTimeSecs() * 1000);
            ps.setString(7, ts.toString());

            ps.setString(8, statsDeviceType);
            ps.setString(9, statsDeviceId);
            ps.setString(10, "null");
            ps.setString(11, "status");
            ps.setString(12, "JSON");

            ts = new Timestamp(System.currentTimeMillis());
            ps.setTimestamp(13, ts);
            ps.setTimestamp(14, ts);

            ps.execute();
            conn.commit();
            conn.close();
            logger.info("Sent connector stats");
        }
        catch (Exception e) {
            logger.log(Level.INFO, e.getMessage(), e);
            // logger.info("Failed to send connector stats. " + e.getMessage());
            rowsProcessed = 0;
        }

        return rowsProcessed;
    }


    private static Connection getSourceConnection(int type) {
        Connection conn = null;
        while (conn == null) {
            try {
                if ( type == Constants.DB_SOURCE_TYPE_MYSQL ) {
                    conn = DriverManager.getConnection(DB_URL, config.getIgnitionDBUser(), config.getIgnitionDBPass());
                } else {
                    conn = DriverManager.getConnection(DB_URL);
                }
                sourceDBConnState = 1;
            } catch(Exception e) {
                logger.log(Level.INFO, e.getMessage(), e);
                conn = null;
                sourceDBConnState = 0;
            }
            if (conn == null) {
                logger.info("Retry source DB connection after 5 seconds.");
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {}
            }
        }

        return conn;
    }

    private static int getSourceMap(List<String> sourceDBColumnNames, ResultSet rs, Map<String, List<Object>> sourceMap) throws Exception {
        int rowCount = 0;
        for (int i=0; i < dbCols.length; i++) {
            sourceMap.put(dbCols[i], new ArrayList<>());
        }

        while (rs.next()) {

            String tagpath = "";
            long tid = 0;
     
            sourceMap.get("EVENTTYPE").add(connectorTypeStr);
            sourceMap.get("FORMAT").add("JSON");
            sourceMap.get("LOGICALINTERFACE_ID").add("null");

            for (int i = 1; i <= sourceDBColumnCount; i++) {
                String colName = sourceDBColumnNames.get(i-1);
                if (rs.getObject(i) != null) {
                    if (colName.equals("tagpath")) {
                        tagpath = rs.getString(i).toLowerCase();
                        sourceMap.get("TAG").add(tagpath);

                    } else if (colName.equals("t_stamp")) {
                        long lastTimeStamp = rs.getLong(i);
                        Timestamp ts = new Timestamp(lastTimeStamp);
                        sourceMap.get("RCV_TIMESTAMP_UTC").add(ts);
                        sourceMap.get("UPDATED_UTC").add(ts);

                    } else if (colName.equals("eventtype")) {
                        sourceMap.get("ETYPE").add(rs.getObject(i));

                    } else if (colName.equals("eventtime")) {
                        sourceMap.get("RCV_TIMESTAMP_UTC").add(rs.getObject(i));
                        sourceMap.get("UPDATED_UTC").add(rs.getObject(i));

                    } else if (colName.equals("tagid")) {
                        tid = rs.getLong(i);
                        sourceMap.get("TAGID").add(tid);

                    } else if (colName.equals("id")) {
                        sourceMap.get("ALARMID").add(rs.getLong(i));

                    } else { 
                        sourceMap.get(colName.toUpperCase()).add(rs.getObject(i));

                    }
                } else {
                    String data = "null";
                    sourceMap.get(colName.toUpperCase()).add(data);
                }
            }


            TagData td = null;
            String idString = clientSite + ":" + tagpath;
            String dId = "";
            String dType = "";

            try {
                td = tagpaths.get(idString);
                dId = td.getDeviceId();
                dType = td.getDeviceType();
                // logger.info(String.format("OLD: dType=%s idStr=%s dId=%s", dType, tagpath, dId));
            } catch(Exception e) {}
            if (td == null) {
                dId = UUID.nameUUIDFromBytes(idString.getBytes()).toString();
                dType = config.getTypeByTagname(tagpath);
                td = new TagData(clientSite, tagpath, dId, dType);
                if (connectorType == Constants.CONNECTOR_DEVICE) {
                    td.setId(tid);
                }
                try {
                    tagpaths.putSafe(idString, td);
                    offsetRecord.setEntityCount(1);
                } catch(Exception e) {}
                // logger.info(String.format("NEW: dType=%s idStr=%s dId=%s", dType, tagpath, dId));
            }
            sourceMap.get("DEVICEID").add(dId);
            sourceMap.get("DEVICETYPE").add(dType);
            if (connectorType == Constants.CONNECTOR_DEVICE) {
                String[] tagelems = tagpath.split("/");
                sourceMap.get("EVT_NAME").add(tagelems[tagelems.length-1]);
            }
            rowCount += 1;
        }

        return rowCount;
    }

    private static void resetDBConnection(Statement stmt, ResultSet rs, Connection conn) throws Exception {
        if (stmt != null) stmt.close();
        if (rs != null) rs.close();
        if (conn != null) conn.close();
        try {
            Thread.sleep(50);
        } catch (Exception e) {}
    }

    private static Connection getDestinationDBConnection(String connStr, String dbUser, String dbPassword) {
        Connection conn = null;
        
        while (conn == null) {
            try {
                conn = DriverManager.getConnection(connStr, dbUser, dbPassword);
                conn.setAutoCommit(false);
                destDBConnState = 1;
            } catch(Exception e) {
                logger.log(Level.INFO, e.getMessage(), e);
                conn = null;
                sourceDBConnState = 0;
            }
            if (conn == null) {
                logger.info("Retry destination DB connection after 5 seconds.");
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {}
            }
        }

        return conn;
    }

    // extract raw data and store in csv file
    public static void extractRawData(String tableName, String csvFilePath) throws Exception {

        FileWriter fw = null;
        Connection conn = null;
        Statement stmt = null;
        int chunkSize = 10000;
        int startRecord = 0;
        String querySql = "";

        try {
            logger.info("Connecting to source to extract data for " + tableName);

            logger.info("Dump extracted data to: " + csvFilePath);
            fw = new FileWriter(csvFilePath);

            int headerWritten = 0;
            while ( true ) {
                logger.info(String.format("TableName:%s StartRecord:%d ChunkSize:%d", tableName, startRecord, chunkSize));
                if (tableName.equals("sqlt_data")) {
                    querySql = String.format("SELECT * FROM sqlt_data_1_2021_05 LIMIT %d, %d", startRecord, chunkSize);
                } else {
                    querySql = String.format("SELECT * FROM sqlth_te LIMIT %d, %d", startRecord, chunkSize);
                }

                conn = getSourceConnection(type);
                stmt = conn.createStatement();

                ResultSet rs = null;
                int gotData = 1;
                try {
                    rs = stmt.executeQuery(querySql);
                } catch (Exception qex) {
                    gotData = 0;
                    if (qex instanceof SQLException) {
                        int errCode = ((SQLException)qex).getErrorCode();
                        if (errCode == 1146) {
                            System.out.println("Table not found in source"); 
                        } else {
                            System.out.println("SQLException: " + qex.getMessage());
                        }
                    }
                }

                if (gotData == 0) {
                    resetDBConnection(stmt, rs, conn);
                    break;
                }


                // Get column count
                final ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
    
                // Open csv file and write column headers
                if (headerWritten == 0) {
                    for (int i = 1; i <= columnCount; i++) {
                        fw.append(rs.getMetaData().getColumnName(i));
                        if ( i < columnCount ) fw.append(",");
                    }
                    fw.append(System.getProperty("line.separator"));
                    headerWritten = 1;
                }
    
                // For each row, loop thru the number of columns and write to the csv file
                int rowCount = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        if (rs.getObject(i) != null) {
                            fw.append(rs.getString(i).replaceAll(",", " "));
                        } else {
                            String data = "null";
                            fw.append(data);
                        }
                        if ( i < columnCount ) fw.append(",");
                    }
                    fw.append(System.getProperty("line.separator"));
                    rowCount += 1;
                }
                fw.flush();
    
                String msg1 = String.format("Data extracted: columns=%d  rows=%d\n", columnCount, rowCount);
                logger.info(msg1);

                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();

                startRecord += chunkSize;
 
            }
        
            fw.close();
            logger.info("Sample Data extraction cycle is complete.");
					
        } catch (Exception ex) {
            throw ex;
        }
    }
}

