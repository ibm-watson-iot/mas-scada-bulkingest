/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.*;
import java.sql.*;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.json.JSONObject;
import org.json.JSONArray;

public class DBHelper {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config connConfig;
    private static int connectorType = Constants.CONNECTOR_DEVICE;
    private static String installDir;
    private static String dataDir;
    private static String clientSite;
    private static String deviceType;
    private static String alarmType;

    private static String startDate;
    private static JSONObject wiotp;
    private static JSONObject ignitionDB;
    private static JSONObject monitorDB;
    private static int sourceDbType;
    private static String sourceDbUrl;
    private static String sourceDbUser;
    private static String sourceDbPass;
    private static int destDbType;
    private static String destDbUrl;
    private static String destDbUser;
    private static String destDbPass;
    private static String entityType;
    private static String pythonPath;
    private static String connectorStatsInsertSQL = "";
    private static String postResponseFile;
    private static String logFile;
    private static String csvFile;
    private static String updateFile;
    private static String enableWebUIFile;
    private static TimeZone localTZ = TimeZone.getDefault();
    private static long fetchInterval = 30L;
    private static long fetchIntervalHistorical = 14400L;
    private static int batchInsertSize = 10000;
    private static int updateFlag = 0;
    private static int httpPort;
    private static int cliPort;
    private static int extractQueryMode = 0;  // QueryMode is not set in extract SQL. Extract all
                                              // 1 - Discrete/Digital queryMode
                                              // 2 - Analog queryMode
 
    public DBHelper(Config connConfig) {
        this.connConfig = connConfig;
        this.connectorType = connConfig.getConnectorType();
        this.installDir = connConfig.getInstallDir();
        this.dataDir = connConfig.getDataDir();
    }

    public String getIgnitionDBSql(long startMilli, long endMilli, int year, int month) {
        String sqlStr = "";
        try {
            if (connectorType == Constants.CONNECTOR_DEVICE) {
                String sqlTemplateFile = dataDir + "/volume/config/deviceSqlTemplate.sql";
                String templateSql = new String(Files.readAllBytes(Paths.get(sqlTemplateFile)));
                sqlStr = String.format(templateSql, year, month, startMilli, endMilli);
            } else {
                Date sDate = new Date(startMilli);
                Date eDate = new Date(endMilli);
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String sDateStr = df.format(sDate);
                String eDateStr = df.format(eDate);
                String sqlTemplateFile = dataDir + "/volume/config/alarmSqlTemplate.sql";
                String templateSql = new String(Files.readAllBytes(Paths.get(sqlTemplateFile)));
                sqlStr = String.format(templateSql, sDateStr, eDateStr);
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return sqlStr; 
    }

    public String[] getMonitorDBCols() {
        if (this.connectorType == 1) {
            String [] dbCols = {"TAGID", "INTVALUE", "FLOATVALUE", "STRINGVALUE", "DATEVALUE", "EVT_NAME",
                "DEVICETYPE", "DEVICEID", "LOGICALINTERFACE_ID", "EVENTTYPE",
                "FORMAT", "RCV_TIMESTAMP_UTC", "UPDATED_UTC"};
            return dbCols;
        }

        String [] dbCols = {"ALARMID", "EVENTID", "ACKBY", "NAME", "ETYPE", "DISPLAYPATH", "PRIORITY", "VALUE",
                "DEVICETYPE", "DEVICEID", "LOGICALINTERFACE_ID", "EVENTTYPE",
                "FORMAT", "RCV_TIMESTAMP_UTC", "UPDATED_UTC"};
        return dbCols;
    }

    public String getMonitorInsertSQL(String eType) {
        String insertSQL;
        if (this.connectorType == 1) {
            insertSQL = "INSERT INTO IOT_" + eType.toUpperCase() +
                " (TAGID, INTVALUE, FLOATVALUE, STRINGVALUE, DATEVALUE, EVT_NAME, DEVICETYPE, DEVICEID, " +
                "LOGICALINTERFACE_ID, EVENTTYPE, FORMAT, RCV_TIMESTAMP_UTC, UPDATED_UTC) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        } else {
            insertSQL = "INSERT INTO IOT_" + eType.toUpperCase() +
                " (ALARMID,EVENTID,ACKBY,NAME,ETYPE,DISPLAYPATH,PRIORITY,VALUE," +
                "DEVICETYPE,DEVICEID,LOGICALINTERFACE_ID,EVENTTYPE,FORMAT,RCV_TIMESTAMP_UTC,UPDATED_UTC) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        }
        return insertSQL;
    }

    public PreparedStatement getMonitorPS(PreparedStatement ps, Map<String, List<Object>> sourceMap, int i) throws Exception {
        if (this.connectorType == 1) {

            ps.setInt(1, Integer.parseInt((sourceMap.get("TAGID").get(i)).toString()));
            if (((sourceMap.get("INTVALUE").get(i)).toString()).equals("null")) {
                ps.setInt(2, 0);
            } else {
                ps.setInt(2, Integer.parseInt((sourceMap.get("INTVALUE").get(i)).toString()));
            }
            if (((sourceMap.get("FLOATVALUE").get(i)).toString()).equals("null")) {
                ps.setDouble(3, 0.0);
            } else {
                ps.setDouble(3, Double.parseDouble((sourceMap.get("FLOATVALUE").get(i)).toString()));
            }
            ps.setString(4, String.valueOf(sourceMap.get("STRINGVALUE").get(i)));
            ps.setString(5, String.valueOf(sourceMap.get("DATEVALUE").get(i)));
            ps.setString(6, String.valueOf(sourceMap.get("EVT_NAME").get(i)));
            ps.setString(7, String.valueOf(sourceMap.get("DEVICETYPE").get(i)));
            ps.setString(8, String.valueOf(sourceMap.get("DEVICEID").get(i)));
            ps.setString(9, String.valueOf(sourceMap.get("LOGICALINTERFACE_ID").get(i)));
            ps.setString(10,String.valueOf(sourceMap.get("EVENTTYPE").get(i)));
            ps.setString(11,String.valueOf(sourceMap.get("FORMAT").get(i)));
            ps.setTimestamp(12,Timestamp.valueOf((sourceMap.get("RCV_TIMESTAMP_UTC").get(i)).toString()));
            ps.setTimestamp(13,Timestamp.valueOf((sourceMap.get("UPDATED_UTC").get(i)).toString()));

        } else {

            ps.setDouble(1, Double.parseDouble((sourceMap.get("ALARMID").get(i)).toString()));
            ps.setString(2, String.valueOf((sourceMap.get("EVENTID").get(i)).toString()));
            ps.setString(3, String.valueOf((sourceMap.get("ACKBY").get(i)).toString()));
            ps.setString(4, String.valueOf((sourceMap.get("NAME").get(i)).toString()));
            if (((sourceMap.get("ETYPE").get(i)).toString()).equals("null")) {
                ps.setDouble(5, 0.0);
            } else {
                ps.setDouble(5, Double.parseDouble((sourceMap.get("ETYPE").get(i)).toString()));
            }
            ps.setString(6, String.valueOf(sourceMap.get("DISPLAYPATH").get(i)));
            if (((sourceMap.get("PRIORITY").get(i)).toString()).equals("null")) {
                ps.setDouble(7, 0.0);
            } else {
                ps.setDouble(7, Double.parseDouble((sourceMap.get("PRIORITY").get(i)).toString()));
            }
            if (((sourceMap.get("VALUE").get(i)).toString()).equals("null")) {
                ps.setDouble(8, 0.0);
            } else {
                ps.setDouble(8, Double.parseDouble((sourceMap.get("VALUE").get(i)).toString()));
            }
            ps.setString(9, String.valueOf(sourceMap.get("DEVICETYPE").get(i)));
            ps.setString(10, String.valueOf(sourceMap.get("DEVICEID").get(i)));
            ps.setString(11, String.valueOf(sourceMap.get("LOGICALINTERFACE_ID").get(i)));
            ps.setString(12,String.valueOf(sourceMap.get("EVENTTYPE").get(i)));
            ps.setString(13,String.valueOf(sourceMap.get("FORMAT").get(i)));
            ps.setTimestamp(14,Timestamp.valueOf((sourceMap.get("RCV_TIMESTAMP_UTC").get(i)).toString()));
            ps.setTimestamp(15,Timestamp.valueOf((sourceMap.get("UPDATED_UTC").get(i)).toString()));

        }
        return ps;
    }

    public String[] getConnectorStatsDBCols() {
        String [] dbCols = {"EXTRACTED", "UPLOADED", "RATE", "ENTITYTYPECOUNT", "ENTITYCOUNT", "EXTSTARTTIME", "EXTENDTIME",
            "DEVICETYPE", "DEVICEID", "LOGICALINTERFACE_ID", "EVENTTYPE", "FORMAT", "RCV_TIMESTAMP_UTC", "UPDATED_UTC"};
        return dbCols;
    }

    public String getConectorStatsInsertSQL(String eType) {
        if (connectorStatsInsertSQL.equals("")) {
            connectorStatsInsertSQL = "INSERT INTO IOT_" + eType.toUpperCase() +
                " (EXTRACTED, UPLOADED, RATE, ENTITYTYPECOUNT, ENTITYCOUNT, EXTSTARTTIME, EXTENDTIME, DEVICETYPE, DEVICEID, " +
                "LOGICALINTERFACE_ID, EVENTTYPE, FORMAT, RCV_TIMESTAMP_UTC, UPDATED_UTC) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        }
        return connectorStatsInsertSQL;
    }

}

