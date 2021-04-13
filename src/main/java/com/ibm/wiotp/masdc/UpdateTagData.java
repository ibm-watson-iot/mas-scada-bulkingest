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
import java.util.Set;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class UpdateTagData {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    static String cacheNameOld;
    static String cacheNameNew;
    static CacheAccess<String, TagDataOld> tagpathsOld;
    static Set<String> tagListOld;
    static CacheAccess<String, TagData> tagpathsNew;
    static Set<String> tagListNew;
    static Config config;
    static String entityType;

    public UpdateTagData(Config config, String cacheNameOld, String cacheNameNew) {
        this.config = config;
        this.cacheNameOld = cacheNameOld;
        this.cacheNameNew = cacheNameNew;
        entityType = config.getEntityType();

        tagpathsOld = JCS.getInstance(cacheNameOld);
        tagListOld = tagpathsOld.getCacheControl().getKeySet();
        logger.info(String.format("Total tags in old cache: %d", tagListOld.size()));

        tagpathsNew = JCS.getInstance(cacheNameNew);
        tagListNew = tagpathsNew.getCacheControl().getKeySet();
        logger.info(String.format("Total tags in new cache: %d", tagListNew.size()));
    }

    public void process() {
        Iterator<String> itOld = tagListOld.iterator();
        while (itOld.hasNext()) {
            String id = itOld.next();
            TagDataOld tdOld = tagpathsOld.get(id);
            if (tdOld == null) {
                continue;
            }
            String tagpath = tdOld.getTagpath();
            String deviceId = tdOld.getDeviceId();
            int deviceStatus = tdOld.getDeviceStatus();
            int dimensionStatus = tdOld.getDimensionStatus();

            System.out.println(String.format("%s : %s - %d %d", tagpath, deviceId, deviceStatus, dimensionStatus));

            // create TagData and add in new cache
            TagData td = new TagData(tagpath, deviceId, entityType);
            td.setDeviceStatus(deviceStatus);
            td.setDimensionStatus(dimensionStatus);
            try {
                tagpathsNew.putSafe(tagpath, td);
            } catch(Exception e) {}
        }

        JCS.shutdown();
    }

}


