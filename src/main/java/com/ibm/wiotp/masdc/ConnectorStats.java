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
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.sql.*;
import java.sql.Types.*;
import org.apache.commons.jcs3.access.CacheAccess;

public class ConnectorStats {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static CacheAccess<String, TagData> tagpaths;
    private static OffsetRecord offsetRecord;
    private static Config config;
    private static DBConnector dbConnector;
    private static int connectorType;
    private static String entityType;
    private static String checkUpdateFile;
    private static String[] masHBDevices = { "masSrcDB", "masDestDB", "masProcessing", "masTags", "masExtStart", "masExtEnd"};
    private static String[] masHBStatNames = {"extracted", "uploaded", "ratePerSec", "total", "timeSec", "timeSec"};
    private static int[] masHBTagIds = {-9001, -9002, -9003, -9004, -9005, -9006};

    public ConnectorStats(Config config, OffsetRecord offsetRecord, CacheAccess<String, TagData> tagpaths) throws Exception {
        if (config == null || offsetRecord == null || tagpaths == null) {
            throw new NullPointerException("config/offsetRecord/tagpaths parameter cannot be null");
        }

        this.config = config;
        this.offsetRecord = offsetRecord;
        this.tagpaths = tagpaths;

        connectorType = config.getConnectorType();
        entityType = config.getEntityType();
        checkUpdateFile = config.getUpdateFile();
    }

    public void start() throws Exception {
        for (int i=0; i < Constants.CONNECTOR_STATS_TAGS; i++) {
            String masHbTag = masHBDevices[i] + "/" + masHBStatNames[i];
            TagData td = new TagData(masHbTag, masHBDevices[i]);
            try {
                tagpaths.putSafe(masHbTag, td);
                offsetRecord.setTagCount(1);
            } catch(Exception e) {}
        }

        try {
            dbConnector = new DBConnector(config, offsetRecord, tagpaths);
            startConnectorStatsThread();
        } catch(Exception e) {
            logger.log(Level.INFO, e.getMessage(), e);
            throw e;
        }
    }

    private static void sendEvent() {
        long[] statValues = {offsetRecord.getProcessedCount(), offsetRecord.getUploadedCount(), offsetRecord.getRate(),
            offsetRecord.getTagCount(), offsetRecord.getStartTimeSecs(), offsetRecord.getEndTimeSecs()};

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
        for (int i=0; i < Constants.CONNECTOR_STATS_TAGS; i++) {
            if (connectorType == Constants.CONNECTOR_DEVICE) {
                statusMap.get("TAGID").add(masHBTagIds[i]);
                statusMap.get("INTVALUE").add(statValues[i]);
                statusMap.get("FLOATVALUE").add(dval);
                statusMap.get("STRINGVALUE").add(strval);
                statusMap.get("DATEVALUE").add(strval);
                statusMap.get("EVT_NAME").add(masHBStatNames[i]);
                statusMap.get("DEVICETYPE").add(entityType);
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
                statusMap.get("DEVICETYPE").add(entityType);
                statusMap.get("DEVICEID").add(masHBDevices[i]);
                statusMap.get("LOGICALINTERFACE_ID").add("null");
                statusMap.get("EVENTTYPE").add("status");
                statusMap.get("FORMAT").add("JSON");
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                statusMap.get("RCV_TIMESTAMP_UTC").add(ts);
                statusMap.get("UPDATED_UTC").add(ts);
            }
        }
        dbConnector.batchInsert(statusMap, Constants.CONNECTOR_STATS_TAGS);
        statusMap.clear();
    }

    private static void startConnectorStatsThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Heartbeat thread: send status");
                    // check for .update file in config directory
                    File f = new File(checkUpdateFile);
                    if (f.exists()) {
                        config.setUpdateFlag(1);
                        break;
                    } else {                        
                        config.setUpdateFlag(0);
                    } 
                    sendEvent();
                    try {
                        Thread.sleep(60000);
                    } catch (Exception e) {}
                }
            }
        };
        logger.info("Starting Connector Stats and heartbeat thread ...");
        new Thread(thread).start();
        logger.info("Connector Stats and heartbeat thread is started.");
        return;
    }

}


