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

    private String type;
    private String name;
    private String host;
    private String port;
    private String user;
    private String pass;
    private String schema;
    private String dbUrl;
    private String ddlQuerySql;
    private String indexQuerySql;

    public MonitorTable(String type, String name, JSONObject datalake) {
        this.type = type;
        this.name = name;
        this.host = datalake.getString("host");
        this.port = datalake.getString("port");
        this.user = datalake.getString("user");
        this.pass = datalake.getString("password");
        this.schema = datalake.getString("schema");
        this.dbUrl = "jdbc:db2://" + host + ":" + port + "/BLUDB:sslConnection=true;";
    }

    public void build() {
        if (this.ddlQuerySql != null) return;
        if (type.equals("device")) {
            // create ddl for device type
            StringBuilder sb =  new StringBuilder("CREATE TABLE ");
            sb.append(this.schema).append(".").append("IOT_").append(name).append(" ( ");
            sb.append("TAGID INTEGER, ");
            sb.append("INTVALUE INTEGER, ");
            sb.append("FLOATVALUE DOUBLE, ");
            sb.append("STRINGVALUE VARCHAR(256), ");
            sb.append("DATEVALUE VARCHAR(256), ");
            sb.append("EVT_NAME VARCHAR(256), "); 
            sb.append("DEVICETYPE VARCHAR(64), "); 
            sb.append("DEVICEID VARCHAR(256), ");
            sb.append("LOGICALINTERFACE_ID VARCHAR(64), ");
            sb.append("EVENTTYPE VARCHAR(64), ");
            sb.append("FORMAT VARCHAR(32), ");
            sb.append("RCV_TIMESTAMP_UTC TIMESTAMP(12), ");
            sb.append("UPDATED_UTC TIMESTAMP(12) )" );
            ddlQuerySql = sb.toString();

            sb =  new StringBuilder("CREATE UNIQUE INDEX DEVICEID_AND_RCV_TIMESTAMP_UTC ON ");
            sb.append(this.schema).append(".").append("IOT_").append(name).append(" ");
            sb.append("(DEVICEID, RCV_TIMESTAMP_UTC)"); 
            indexQuerySql = sb.toString();

        } else {
            // create ddl for alarm type
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
            sb.append("DEVICETYPE VARCHAR(64), "); 
            sb.append("DEVICEID VARCHAR(256), ");
            sb.append("LOGICALINTERFACE_ID VARCHAR(64), ");
            sb.append("EVENTTYPE VARCHAR(64), ");
            sb.append("FORMAT VARCHAR(32), ");
            sb.append("RCV_TIMESTAMP_UTC TIMESTAMP(12), ");
            sb.append("UPDATED_UTC TIMESTAMP(12) )" );
            ddlQuerySql = sb.toString();

            sb =  new StringBuilder("CREATE UNIQUE INDEX DEVICEID_AND_RCV_TIMESTAMP_UTC ON ");
            sb.append(this.schema).append(".").append("IOT_").append(name).append(" ");
            sb.append("(DEVICEID, RCV_TIMESTAMP_UTC)"); 
            indexQuerySql = sb.toString();

        }
    }

    public String getDDLQuerySql() {
        return ddlQuerySql;
    }

    public String getIndexQuerySql() {
        return indexQuerySql;
    }

    public void create() {
        Connection con;
        Statement stmt;

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            con = DriverManager.getConnection(dbUrl, user, pass);
            DatabaseMetaData dbm = con.getMetaData();
            ResultSet rs = dbm.getTables(null, this.schema, "IOT_" + this.name.toUpperCase(), null);
            if (rs.next()) {
                logger.info("Table " + "IOT_" + this.name.toUpperCase() + " exists.");
            } else {
                con.setAutoCommit(false);
                stmt = con.createStatement();
                stmt.executeUpdate(ddlQuerySql);
                stmt.close();
                con.commit();
            }
            con.close();
        }
        catch (Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }
    }

    public void indexTable() {
        Connection con;
        Statement stmt;

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            con = DriverManager.getConnection(dbUrl, user, pass);
            DatabaseMetaData dbm = con.getMetaData();
            ResultSet rs = dbm.getTables(null, this.schema, "IOT_" + this.name.toUpperCase(), null);
            if (rs.next()) {
                logger.info("Table " + "IOT_" + this.name.toUpperCase() + " exists.");
                con.setAutoCommit(false);
                stmt = con.createStatement();
                stmt.executeUpdate(indexQuerySql);
                stmt.close();
                con.commit();
            } else {
                logger.info("Can not index an non-existence Table " + "IOT_" + this.name.toUpperCase());
            }
            con.close();
        }
        catch (Exception ex) {
            logger.info("Table index error: " + "IOT_" + this.name.toUpperCase() + " : " + ex.getMessage());
        }
    }
}

