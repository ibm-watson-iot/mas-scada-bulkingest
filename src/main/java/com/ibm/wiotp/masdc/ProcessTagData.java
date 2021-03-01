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
import java.sql.*;
import java.sql.Types.*;
import org.apache.commons.jcs3.access.CacheAccess;

public class ProcessTagData {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    static CacheAccess<String, TagData> tagpaths;
    static OffsetRecord offsetRecord;
    static Config config;
    static DBConnector dbConnector;

    static int connectorType;
    static String entityType;

    public ProcessTagData(Config config, OffsetRecord offsetRecord, CacheAccess<String, TagData> tagpaths) throws Exception {
        if (config == null || offsetRecord == null || tagpaths == null) {
            throw new NullPointerException("config/offsetRecord/tagpaths parameter cannot be null");
        }

        this.config = config;
        this.offsetRecord = offsetRecord;
        this.tagpaths = tagpaths;

        connectorType = config.getConnectorType();
        entityType = config.getEntityType();
    }

    public void start() throws Exception {
        int normalStop = 0;
        while(true) {
            if (config.getUpdateFlag() == 1) break;
            try {
                dbConnector = new DBConnector(config, offsetRecord, tagpaths);
                dbConnector.extractAndUpload();
                normalStop = 1;
            } catch(Exception e) {
                logger.log(Level.INFO, e.getMessage(), e);
            }
            if (normalStop == 0) {
                // restart after 30 seconds
                try {
                    Thread.sleep(30000);
                } catch (Exception e) {}
            }
        }
    }

}


