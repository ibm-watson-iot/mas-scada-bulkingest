/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.logging.*;
import org.json.JSONObject;
import java.sql.*;

public class MonitorTable {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private Config config;
    private JSONObject datalake;
    private String type;
    private String host;
    private String port;
    private String user;
    private String pass;
    private String schema;
    private String dbUrl;
    private String indexQuerySql;
    private String statsDDLQuerySql;

    public MonitorTable(Config config) throws Exception {
        if (config == null) {
            throw new NullPointerException("config parameter cannot be null");
        }

        this.config = config;

        this.type = config.getConnectorTypeStr();
        this.datalake = config.getMonitorConfig();
        this.host = datalake.getString("host");
        this.port = datalake.getString("port");
        this.user = datalake.getString("user");
        this.pass = datalake.getString("password");
        this.schema = datalake.getString("schema");
        this.dbUrl = "jdbc:db2://" + host + ":" + port + "/BLUDB:sslConnection=true;";
    }

    public String getDeviceDDL(String name) {
        StringBuilder sb =  new StringBuilder("CREATE TABLE ");
        sb.append(this.schema).append(".").append("IOT_").append(name).append(" ( ");
        sb.append("TAGID INTEGER, ");
        sb.append("INTVALUE INTEGER, ");
        sb.append("FLOATVALUE DOUBLE, ");
        sb.append("STRINGVALUE VARCHAR(256), ");
        sb.append("DATEVALUE VARCHAR(256), ");
        sb.append("EVT_NAME VARCHAR(256), "); 
        sb.append("TAG VARCHAR(256), "); 
        sb.append("DEVICETYPE VARCHAR(64), "); 
        sb.append("DEVICEID VARCHAR(256), ");
        sb.append("LOGICALINTERFACE_ID VARCHAR(64), ");
        sb.append("EVENTTYPE VARCHAR(64), ");
        sb.append("FORMAT VARCHAR(32), ");
        sb.append("RCV_TIMESTAMP_UTC TIMESTAMP(12), ");
        sb.append("UPDATED_UTC TIMESTAMP(12) )" );
        String deviceDDL = sb.toString();
        return deviceDDL;
    }

    public String getIndexSQL(String name) {
        StringBuilder sb =  new StringBuilder("CREATE UNIQUE INDEX DEVICEID_AND_RCV_TIMESTAMP_UTC ON ");
        sb.append(this.schema).append(".").append("IOT_").append(name).append(" ");
        sb.append("(DEVICEID, RCV_TIMESTAMP_UTC)"); 
        String indexSql = sb.toString();
        return indexSql;
    }

    public String getAlarmDDL(String name) {
        StringBuilder sb =  new StringBuilder("CREATE TABLE ");
        sb.append(this.schema).append(".").append("IOT_").append(name).append(" ( ");
        sb.append("ALARMID INTEGER, ");
        sb.append("EVENTID VARCHAR(256), ");
        sb.append("DISPLAYPATH VARCHAR(256), ");
        sb.append("PRIORITY INTEGER, ");
        sb.append("ETYPE INTEGER, ");
        sb.append("NAME VARCHAR(256), ");
        sb.append("ACKBY VARCHAR(256), ");
        sb.append("VALUE DOUBLE, ");
        sb.append("TAG VARCHAR(256), ");
        sb.append("DEVICETYPE VARCHAR(64), "); 
        sb.append("DEVICEID VARCHAR(256), ");
        sb.append("LOGICALINTERFACE_ID VARCHAR(64), ");
        sb.append("EVENTTYPE VARCHAR(64), ");
        sb.append("FORMAT VARCHAR(32), ");
        sb.append("RCV_TIMESTAMP_UTC TIMESTAMP(12), ");
        sb.append("UPDATED_UTC TIMESTAMP(12) )" );
        String alarmDDL = sb.toString();
        return alarmDDL;
    }

    public String getConnectorStatsDDL(String name) {
        StringBuilder sb =  new StringBuilder("CREATE TABLE ");
        sb.append(this.schema).append(".").append("IOT_").append(name).append(" ( ");
        sb.append("EXTRACTED DOUBLE, ");
        sb.append("UPLOADED DOUBLE, ");
        sb.append("RATE DOUBLE, ");
        sb.append("ENTITYTYPECOUNT DOUBLE, ");
        sb.append("ENTITYCOUNT DOUBLE, ");
        sb.append("EXTSTARTTIME VARCHAR(32), ");
        sb.append("EXTENDTIME VARCHAR(32), ");
        sb.append("DEVICETYPE VARCHAR(64), "); 
        sb.append("DEVICEID VARCHAR(64), ");
        sb.append("LOGICALINTERFACE_ID VARCHAR(64), ");
        sb.append("EVENTTYPE VARCHAR(64), ");
        sb.append("FORMAT VARCHAR(32), ");
        sb.append("RCV_TIMESTAMP_UTC TIMESTAMP(12), ");
        sb.append("UPDATED_UTC TIMESTAMP(12) )" );
        String statsDDL = sb.toString();
        return statsDDL;
    }

    public int createTable(String tableName, String ddlStr) throws Exception {
        Connection con;
        Statement stmt;
        int created = 0;

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            con = DriverManager.getConnection(dbUrl, user, pass);
            DatabaseMetaData dbm = con.getMetaData();
            ResultSet rs = dbm.getTables(null, this.schema, tableName.toUpperCase(), null);
            if (rs.next()) {
                logger.info("Table " + tableName.toUpperCase() + " exists.");
            } else {
                con.setAutoCommit(false);
                stmt = con.createStatement();
                stmt.executeUpdate(ddlStr);
                stmt.close();
                con.commit();
                created = 1;
            }
            con.close();
        }
        catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
            throw(ex);
        }

        return created;
    }

    public void indexTable(String tableName, String ddlStr) {
        Connection con;
        Statement stmt;

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            con = DriverManager.getConnection(dbUrl, user, pass);
            DatabaseMetaData dbm = con.getMetaData();
            ResultSet rs = dbm.getTables(null, this.schema, tableName.toUpperCase(), null);
            if (rs.next()) {
                logger.info("Table " + tableName.toUpperCase() + " exists.");
                con.setAutoCommit(false);
                stmt = con.createStatement();
                stmt.executeUpdate(indexQuerySql);
                stmt.close();
                con.commit();
            } else {
                logger.info("Can not index an non-existence Table " + tableName.toUpperCase());
            }
            con.close();
        }
        catch (Exception ex) {
            logger.info("Table index error: " + tableName.toUpperCase() + " : " + ex.getMessage());
        }
    }
}

