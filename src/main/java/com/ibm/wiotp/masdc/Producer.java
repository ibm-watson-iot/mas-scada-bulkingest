/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.wiotp.masdc;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.*;
import java.sql.Types.*;
import java.util.Map;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
    

public class Producer implements Callable<Boolean> {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static String monitorConnStr = "";
    private static String user = "";
    private static String pass = "";
    private static int connState = 0;

    private Config config;
    private static DBHelper dbHelper = null;
    private OffsetRecord offsetRecord;
    private Map<String, List<Object>> sourceMap;
    private long totalRows;
    private int batchInsertSize = 10000;
    private String entityType;


    public Producer(Config config, OffsetRecord offsetRecord, Map<String, List<Object>> sourceMap, long totalRows) {
        this.config = config;
        this.offsetRecord = offsetRecord;
        this.sourceMap = sourceMap;
        this.totalRows = totalRows;
        this.batchInsertSize = config.getBatchInsertSize();
        this.monitorConnStr = config.getMonitorDBUrl();
        this.user = config.getMonitorDBUser();
        this.pass = config.getMonitorDBPass();
        this.dbHelper = new DBHelper(config);
        this.entityType = config.getEntityType();
    }

    @Override
    public Boolean call() throws Exception {
        int rowsProcessed = 0;
        int batchCount = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");

            long cycleStartTimeMillis = System.currentTimeMillis();
            long cycleEndTimeMillis = 0;
            long rate = 0;

            String insertSQL = dbHelper.getMonitorInsertSQL(entityType);
            logger.info("SQL Stmt: " + insertSQL);

            Connection conn = getDBConnection();
            PreparedStatement ps = conn.prepareStatement(insertSQL);

            for (int i=0; i<totalRows; i++) {
                try {
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

            if (rowsProcessed > 0) {
                // calculate record processing rate
                cycleEndTimeMillis = System.currentTimeMillis();
                long timeDiff = (cycleEndTimeMillis - cycleStartTimeMillis);
                rate = rowsProcessed * 1000 / timeDiff;
            }
            logger.info(String.format("Upload stats: uploaded:%d rate:%d", rowsProcessed, rate));
        }
        catch (Exception e) {
            logger.info("Caught exception: rowsProcessed:" + rowsProcessed + " batchCount:" + batchCount);
        }

        offsetRecord.setUploadedCount(rowsProcessed);
        return true;
    }

    private static Connection getDBConnection() {
        Connection conn = null;
        while (conn == null) {
            try {
                conn = DriverManager.getConnection(monitorConnStr, user, pass);
                conn.setAutoCommit(false);
                connState = 1;
            } catch(Exception e) {
                logger.log(Level.INFO, e.getMessage(), e);
                logger.info("Retry destination DB connection");
                conn = null;
                connState = 0;
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

