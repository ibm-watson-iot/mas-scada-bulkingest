/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.sql.*;
import java.sql.Types.*;
import org.apache.commons.jcs3.access.CacheAccess;

public class TransformData {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    static Config config;
    static int connectorType;
    static String connectorTypeStr;
    static OffsetRecord offsetRecord;
    static CacheAccess<String, TagData> tagpaths;
    static List<String> sourceDBColumnNames;
    static int sourceDBColumnCount;
    static ResultSet rs;
    static Map<String, List<Object>> sourceMap = null;
    static String[] dbCols;
    static int rowCount = 0;

    public TransformData(Config config, String[] dbCols, OffsetRecord offsetRecord, CacheAccess<String, TagData> tagpaths, List<String> sourceDBColumnNames, ResultSet rs) throws Exception {
        if (config == null || dbCols == null || offsetRecord == null || tagpaths == null || sourceDBColumnNames == null || rs == null) {
            throw new NullPointerException("config/dbCold/tagpaths/sourceDBColumnNames/rs parameter cannot be null");
        }

        this.config = config;
        this.connectorType = config.getConnectorType();
        this.connectorTypeStr = config.getConnectorTypeStr();
        this.dbCols = dbCols;
        this.sourceDBColumnNames = sourceDBColumnNames;
        this.sourceDBColumnCount = sourceDBColumnNames.size();
        this.rs = rs;
        this.sourceMap = new HashMap<String, List<Object>>();
    }

    public void set() throws Exception {
        for (int i=0; i < dbCols.length; i++) {
            sourceMap.put(dbCols[i], new ArrayList<>());
        }

        while (rs.next()) {
            sourceMap.get("EVENTTYPE").add(connectorTypeStr);
            sourceMap.get("FORMAT").add("JSON");
            sourceMap.get("LOGICALINTERFACE_ID").add("null");
    
            for (int i = 1; i <= sourceDBColumnCount; i++) {
                String colName = sourceDBColumnNames.get(i-1);
                if (rs.getObject(i) != null) {
                    if (colName.equals("tagpath")) {
                        String tagpath = rs.getString(i);
                        String data = UUID.nameUUIDFromBytes(tagpath.getBytes()).toString();
                        String dType = config.getTypeByTagname(tagpath);
                        TagData td = new TagData(tagpath, data, dType);
                        try {
                            tagpaths.putSafe(tagpath, td);
                            offsetRecord.setEntityCount(1);
                        } catch(Exception e) {}
                        sourceMap.get("DEVICETYPE").add(dType);
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
    }

    public Map<String, List<Object>> get() {
        return this.sourceMap;
    }

    public int getRowCount() {
        return this.rowCount;
    }

}


