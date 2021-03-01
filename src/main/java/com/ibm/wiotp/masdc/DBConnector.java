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
import java.util.ArrayList;
import java.util.UUID;
import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.jcs3.access.CacheAccess;


public class DBConnector {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config = null;
    private static OffsetRecord offsetRecord = null;
    private static int type;
    private static String DB_URL;
    private static String[] dbCols;
    private static String entityType;
    private static int runMode;
    private static String csvFile;
    private static int connectorType;
    private static String connectorTypeStr;
    private static CacheAccess<String, TagData> tagpaths;
    private static int batchInsertSize = 10000;
    private static int sourceDBConnState = 0;
    private static int destDBConnState = 0;
    private static int sourceDBColumnCount = 0;

    public DBConnector(Config config, OffsetRecord offsetRecord, CacheAccess<String, TagData> tagpaths) throws Exception {
        if (config == null || offsetRecord == null || tagpaths == null) {
            throw new NullPointerException("config/offsetRecord/tagpaths parameter cannot be null");
        }

        this.config = config;
        this.offsetRecord = offsetRecord;
        this.tagpaths = tagpaths;

        type = config.getIgnitionDBType();
        DB_URL = config.getIgnitionDBUrl();
        dbCols = config.getMonitorDBCols();
        entityType = config.getEntityType();
        runMode = config.getRunMode();
        csvFile = config.getCSVFile();
        connectorType = config.getConnectorType();
        connectorTypeStr = config.getConnectorTypeStr();
        batchInsertSize = config.getBatchInsertSize();
    }    


    // Extract and Upload data to data lake
    public static void extractAndUpload() throws Exception {

        FileWriter fw = null;
        Connection conn = null;
        Statement stmt = null;
        int noUpload = 1;

        try {
            logger.info("Connecting to source to extract data for " + entityType);

            // retrieve records
            long cycleStartTimeMillis = 0;
            long cycleEndTimeMillis = 0;
            long cycleTime = 0;
            List<String> sourceDBColumnNames = new ArrayList<String>();

            while ( true ) {

                // check update flag is set wait for 60 seconds and check again
                if (config.getUpdateFlag() == 1) {
                    logger.info("Update flag is set. Wait for 60 seconds for process to be stopped.");
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
                    sourceMap.clear();
                    int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_NO_DATA);
                    long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
                    logger.info(String.format("No Data extracted: currCount=%d waitTimeMilli=%d currTagCount=%d\n", 
                        currentTotalCount, waitTime, offsetRecord.getTagCount()));
                    try {
                        Thread.sleep(waitTime);
                    } catch (Exception e) {}
                    continue;
                }
    
                logger.info(String.format("Data extracted: cols=%d rows=%d currCount=%d currTagCount=%d\n", 
                    sourceDBColumnCount, rowCount, currentTotalCount, offsetRecord.getTagCount()));

                int nuploaded = 0;
                if (rowCount > 0) {
                    nuploaded = batchInsert(sourceMap, rowCount);
                    offsetRecord.setUploadedCount(nuploaded);
                }

                sourceMap.clear();

                int waitFlag = offsetRecord.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
                long waitTime = offsetRecord.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);

                // calculate record processing rate
                cycleEndTimeMillis = System.currentTimeMillis();
                long timeDiff = (cycleEndTimeMillis - cycleStartTimeMillis);
                long rate = rowCount * 1000 / timeDiff;
                offsetRecord.setRate(rate);

                logger.info(String.format("Cycle stats: extracted:%d uploaded=%d rate=%d waitTimeMilli=%d currTagCount=%d\n", 
                        rowCount, nuploaded, rate, waitTime, offsetRecord.getTagCount()));

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

    public static int batchInsert(Map<String, List<Object>> sourceMap, long totalRows) {
        int rowsProcessed = 0;
        int batchCount = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");

            String insertSQL = config.getMonitorInsertSQL();
            logger.info("SQL Stmt: " + insertSQL);

            Connection conn = getDestinationDBConnection();
            PreparedStatement ps = conn.prepareStatement(insertSQL);

            for (int i=0; i<totalRows; i++) {
                try {
                    ps = config.getMonitorPS(ps, sourceMap, i);
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
                logger.info("Retry source DB connection");
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
            sourceMap.get("DEVICETYPE").add(entityType);
            sourceMap.get("EVENTTYPE").add(connectorTypeStr);
            sourceMap.get("FORMAT").add("JSON");
            sourceMap.get("LOGICALINTERFACE_ID").add("null");
    
            for (int i = 1; i <= sourceDBColumnCount; i++) {
                String colName = sourceDBColumnNames.get(i-1);
                if (rs.getObject(i) != null) {
                    if (colName.equals("tagpath")) {
                        String tagpath = rs.getString(i);
                        String data = UUID.nameUUIDFromBytes(tagpath.getBytes()).toString();
                        TagData td = new TagData(tagpath, data);
                        try {
                            tagpaths.putSafe(tagpath, td);
                            offsetRecord.setTagCount(1);
                        } catch(Exception e) {}
                        sourceMap.get("DEVICEID").add(data);
                        String[] tagelems = tagpath.split("/");
                        if (connectorType == Constants.CONNECTOR_DEVICE) {
                            sourceMap.get("EVT_NAME").add(tagelems[tagelems.length-1]);
                        }
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

    private static Connection getDestinationDBConnection() {
        Connection conn = null;
        while (conn == null) {
            try {
                String monitorConnStr = config.getMonitorDBUrl();
                conn = DriverManager.getConnection(monitorConnStr, config.getMonitorDBUser(), config.getMonitorDBPass());
                conn.setAutoCommit(false);
                destDBConnState = 1;
            } catch(Exception e) {
                logger.log(Level.INFO, e.getMessage(), e);
                logger.info("Retry destination DB connection");
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

}

