/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
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

public class Config {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static String installDir = "";
    private static String dataDir = "";
    private static int runMode = Constants.PRODUCTION;
    private static int connectorType = Constants.CONNECTOR_DEVICE;
    private static String connectorTypeStr;
    private static JSONObject connConfig;
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
    private static String insertSQL = "";
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

 
    public Config(JSONObject connConfig) {
        this.connConfig = connConfig;
    }

    public Config(String installDir, String dataDir, String connectorTypeStr) throws Exception {
        this.installDir = installDir;
        this.dataDir = dataDir;
        this.connectorTypeStr = connectorTypeStr;
        this.connectorType = Constants.CONNECTOR_DEVICE;
        if (connectorTypeStr.equals("alarm")) {
            this.connectorType = Constants.CONNECTOR_ALARM;
        }
        String connectionConfigFile = dataDir + "/volume/config/connection.json";
        String fileContent = new String(Files.readAllBytes(Paths.get(connectionConfigFile)));
        connConfig = new JSONObject(fileContent);
    }

    public void set() throws Exception {
        clientSite = connConfig.getString("clientSite");
        deviceType = connConfig.getString("deviceType");
        alarmType = connConfig.getString("alarmType");

        long curTimeMillis = System.currentTimeMillis();
        Date date = new Date(curTimeMillis);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentDateStr = df.format(date);
        startDate = connConfig.optString("startDate", currentDateStr);

        // if configured startDate is in future, set startDate to current date
        DateUtil du = new DateUtil(startDate);
        if (du.getTimeMilli() > curTimeMillis) {
            logger.info("Specified start date " + startDate + " is in future. Setting startDate to " + currentDateStr);
            startDate = currentDateStr;
        }

        runMode = connConfig.optInt("runMode", 0);
        fetchInterval = connConfig.optLong("fetchInterval", 30L);
        fetchIntervalHistorical = connConfig.optLong("fetchIntervalHistorical", 14400L);

        if (this.connectorType == Constants.CONNECTOR_DEVICE) {
            fetchInterval = setLongValue("deviceFetchInterval", fetchInterval);
            fetchIntervalHistorical = setLongValue("deviceFetchIntervalHistorical", fetchIntervalHistorical);
            startDate = setStringValue("deviceStartDate", startDate);
        } else {
            fetchInterval = setLongValue("alarmFetchInterval", fetchInterval);
            fetchIntervalHistorical = setLongValue("alarmFetchIntervalHistorical", fetchIntervalHistorical);
            startDate = setStringValue("alarmStartDate", startDate);
        }

        batchInsertSize  = connConfig.optInt("batchInsertSize", 10000);

        if (this.connectorType == Constants.CONNECTOR_DEVICE) {
            httpPort  = connConfig.optInt("httpPort", 5080);
            cliPort = connConfig.optInt("cliPort", 4550);
        } else {
            httpPort  = connConfig.optInt("httpPort", 5081);
            cliPort = connConfig.optInt("cliPort", 4551);
        }

        wiotp = connConfig.getJSONObject("wiotp");
        ignitionDB = connConfig.getJSONObject("ignition");
        monitorDB = connConfig.getJSONObject("monitor");

        entityType = deviceType;
        if (connectorType == Constants.CONNECTOR_ALARM) {
            entityType = alarmType;
        }

        if (installDir.equals("")) {
            logFile = entityType + "_connector.log";
        } else {
            logFile = installDir + "/volume/logs/" + entityType + "_connector.log";
        }
        if (runMode == Constants.PRODUCTION) {
            postResponseFile = "";
        } else {
            if (installDir.equals("")) {
                postResponseFile = entityType + "_post.log";
            } else {
                postResponseFile = installDir + "/volume/logs/" + entityType + "_post.log";
            }
        }

        sourceDbType = setIgnitionDbParams(ignitionDB);
        destDbType = setMonitorDbParams(monitorDB);

        csvFile = dataDir + "/volume/data/" + entityType + ".csv";
        updateFile = dataDir + "/volume/config/.upgrade";
        enableWebUIFile = dataDir + "/volume/config/.enableWebUI";

        String osname = System.getProperty("os.name");
        if ( osname.startsWith("Windows")) {
            if (installDir.equals("")) {
                pythonPath = "python";
            } else {
                pythonPath = installDir + "/python-3.7.5/python.exe";
            }
        } else {
            pythonPath = "python3";
        }
    }

    public String getLogFile() {
        return logFile;
    }

    public String getCSVFile() {
        return csvFile;
    }

    public String getUpdateFile() {
        return updateFile;
    }

    public String getEnableWebUIFile() {
        return enableWebUIFile;
    }

    public void setUpdateFlag(int flag) {
        updateFlag = flag;
    }

    public int getUpdateFlag() {
        return updateFlag;
    }

    public int getRunMode() {
        return runMode;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public long getFetchIntervalHistorical() {
        return fetchIntervalHistorical;
    }

    public int getBatchInsertSize() {
        return batchInsertSize;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getCLIPort() {
        return cliPort;
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getPostResponseFile() {
        return postResponseFile;
    }

    public int getConnectorType() {
        return connectorType;
    }

    public String getConnectorTypeStr() {
        return connectorTypeStr;
    }

    public String getClientSite() {
        return clientSite;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setFetchInterval(long interval) {
        fetchInterval = interval;
    }

    public void setFetchIntervalHistorical(long interval) {
        fetchIntervalHistorical = interval;
    }

    public void setStartDate(String newStartDate) {
        startDate = newStartDate;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getIgnitionDBUrl() {
        return sourceDbUrl;
    }

    public String getIgnitionDBUser() {
        return sourceDbUser;
    }

    public String getIgnitionDBPass() {
        return sourceDbPass;
    }

    public int getIgnitionDBType() {
        return sourceDbType;
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


    public String getMonitorDBUrl() {
        return destDbUrl;
    }

    public String getMonitorDBUser() {
        return destDbUser;
    }

    public String getMonitorDBPass() {
        return destDbPass;
    }

    public int getMonitorDBType() {
        return destDbType;
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

    public String getMonitorInsertSQL() {
        if (this.insertSQL.equals("")) {
            if (this.connectorType == 1) {
                this.insertSQL = "INSERT INTO IOT_" + this.entityType.toUpperCase() +
                    " (TAGID, INTVALUE, FLOATVALUE, STRINGVALUE, DATEVALUE, EVT_NAME, DEVICETYPE, DEVICEID, " +
                    "LOGICALINTERFACE_ID, EVENTTYPE, FORMAT, RCV_TIMESTAMP_UTC, UPDATED_UTC) VALUES " +
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            } else {
                this.insertSQL = "INSERT INTO IOT_" + this.entityType.toUpperCase() +
                    " (ALARMID,EVENTID,ACKBY,NAME,ETYPE,DISPLAYPATH,PRIORITY,VALUE," +
                    "DEVICETYPE,DEVICEID,LOGICALINTERFACE_ID,EVENTTYPE,FORMAT,RCV_TIMESTAMP_UTC,UPDATED_UTC) " +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            }
        }
        return this.insertSQL;
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


    public String getPythonPath() {
        return pythonPath;
    }

    public JSONObject getWiotpConfig() {
        return wiotp;
    }

    public JSONObject getIgnitionConfig() {
        return ignitionDB;
    }

    public JSONObject getMonitorConfig() {
        return monitorDB;
    }

    private int setIgnitionDbParams(JSONObject ignitionDB) {
        int type = 1;

        String dbType = ignitionDB.getString("dbtype"); // mysql and mssql
        String sourceHost = ignitionDB.getString("host");
        String sourcePort = ignitionDB.getString("port");
        String sourceDatabase = ignitionDB.getString("database");
        String sourceSchema = ignitionDB.getString("schema");
        sourceDbUser = ignitionDB.getString("user");
        sourceDbPass = ignitionDB.getString("password");

        if ( dbType.compareTo("mysql") == 0 ) {
            sourceDbUrl = "jdbc:mysql://" + sourceHost + "/" + sourceSchema;
        } else {
            sourceDbUrl = "jdbc:sqlserver://"+sourceHost+":"+sourcePort+";databaseName="+sourceDatabase+";user="+
                    sourceDbUser+";password="+sourceDbPass;
            type = 2;
        }
        return type;
    }

    private int setMonitorDbParams(JSONObject monitorDB) {
        int type = 1;

        String dbType = monitorDB.getString("dbtype"); // db2 and postgres
        String destHost = monitorDB.getString("host");
        String destPort = monitorDB.getString("port");
        String destSchema = monitorDB.getString("schema");
        destDbUser = monitorDB.getString("user");
        destDbPass = monitorDB.getString("password");

        // if ( dbType.compareTo("db2") == 0 ) {
            destDbUrl = "jdbc:db2://" + destHost + ":" + destPort + "/BLUDB:sslConnection=true;";
        // }
        return type;
    }

    private Calendar dateToCal(String dateStr) {
        Calendar cal = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(dateStr);
            cal = Calendar.getInstance(localTZ);
            cal.setTime(date);
        } catch(Exception ex) {}
        return cal;
    }

    private long setLongValue(String propName, long defaultValue) {
        long value = 0;
        try {
            value = connConfig.getLong(propName);
        } catch (Exception e) {}
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

    private String setStringValue(String propName, String defaultValue) {
        String value = null;
        try {
            value = connConfig.getString(propName);
        } catch (Exception e) {}
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

}

