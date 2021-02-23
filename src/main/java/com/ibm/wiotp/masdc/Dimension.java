/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.logging.*;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.Set;
import java.util.Iterator;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class Dimension {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private String client;
    private String type;
    private String name;
    private JSONObject wiotp;
    private String baseUrl;
    private String dimensionalAPI;
    private RestClient restClient;
    private Config config;

    public Dimension(Config config, String client, String type, String name, JSONObject wiotp) {
        this.config = config;
        this.client = client;
        this.type = type;
        this.name = name;
        this.wiotp = wiotp;
        this.baseUrl = "https://api-" + wiotp.getString("geo") + ".connectedproducts.internetofthings.ibmcloud.com/api";
        this.dimensionalAPI = "/master/v1/" + wiotp.getString("tenantId") + "/entityType/" + name + "/dimensional";
        this.restClient = new RestClient(baseUrl, 2, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void apply(CacheAccess<String, TagData> tagpaths) {
        int dimAdded = 0;
        int dimRegistered = 0;
        int batchCount = 0;
        int totalCount = tagpaths.getCacheControl().getSize();
        JSONArray dimensionObj = null;
        logger.info(String.format("Dimension Data: Tagpath count: %d", totalCount));

        Set<String> tagList = tagpaths.getCacheControl().getKeySet();
        Iterator<String> it = tagList.iterator();
        while (it.hasNext()) {
            if (batchCount == 0 ) {
                dimensionObj = new JSONArray();
                batchCount = 1;
            }

            String id = it.next();
            TagData td = tagpaths.get(id);
            if (td == null) {
                totalCount = totalCount - 1;
                continue;
            }
            String deviceId = td.getDeviceId();
            String tagpath = td.getTagpath();
            int dimensionStatus = td.getDimensionStatus();

            if (dimensionStatus == 0) {
                logger.info("Add dimension: tagpath: " + tagpath + "    Dimention ID: " + deviceId);
                dimensionObj.put(createDimItem(deviceId, "CLIENT", "LITERAL", client));
                dimensionObj.put(createDimItem(deviceId, "TAGPATH", "LITERAL", tagpath));
                batchCount += 2;
                td.setDimensionStatus(1);
    
                String[] tagLevels = tagpath.split("/");
                int levelCount = 0;
                for (String level : tagLevels) {
                    dimensionObj.put(createDimItem(deviceId, "LEVEL_" + Integer.toString(levelCount) , "LITERAL", level));
                    levelCount += 1;
                    batchCount += 1;
                }
                dimAdded += 1;
            } else {
                logger.fine("Dimension was already added: tagpath: " + tagpath + "    Dimention ID: " + deviceId);
                dimRegistered += 1;
            }
 
            if (batchCount >= 80 || totalCount == 1) {
                try {
                    // invoke API to create dimensional data
                    logger.fine("DimensionObj:   " + dimensionObj.toString());
                    restClient.post(dimensionalAPI, dimensionObj.toString());
                    logger.info(String.format("Dimension POST Status Code: %d", restClient.getResponseCode()));
                } catch(Exception ex) {
                    logger.log(Level.INFO, ex.getMessage(), ex);
                }
                batchCount = 0;
            }
            totalCount = totalCount - 1;
        }
        logger.info(String.format("DimensionCycle TotalRemaining:%d New:%d AlreadyRegistered:%d", totalCount, dimAdded, dimRegistered));
    }

    private static JSONObject createDimItem(String id, String name, String type, String value) {
        JSONObject dimItem = new JSONObject();
        dimItem.put("id", id);
        dimItem.put("name", name);
        dimItem.put("type", type);
        dimItem.put("value", value);
        return dimItem;
    }

}

