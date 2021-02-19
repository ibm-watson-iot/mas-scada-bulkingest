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

    public void apply(HashMap<String, String> tagpaths) {
        int batchCount = 1;
        int totalCount = tagpaths.size();
        JSONArray dimensionObj = null;
        logger.info(String.format("Dimension Data: Tagpath count: %d", totalCount));
        for (String tagpath : tagpaths.keySet()) {
            if (batchCount == 1 ) dimensionObj = new JSONArray();
            String id = tagpaths.get(tagpath);

            logger.info("tagpath: " + tagpath + "    Dimention ID: " + id);

            dimensionObj.put(createDimItem(id, "CLIENT", "LITERAL", client));
            dimensionObj.put(createDimItem(id, "TAGPATH", "LITERAL", tagpath));
            batchCount += 2;

            String[] tagLevels = tagpath.split("/");
            int levelCount = 0;
            for (String level : tagLevels) {
                dimensionObj.put(createDimItem(id, "LEVEL_" + Integer.toString(levelCount) , "LITERAL", level));
                levelCount += 1;
                batchCount += 1;
            }

            totalCount = totalCount - 1;
 
            if (batchCount >= 80 || totalCount <= 1) {
                try {
                    // invoke API to create dimensional data
                    logger.info("DimensionObj:   " + dimensionObj.toString());
                    restClient.post(dimensionalAPI, dimensionObj.toString());
                    logger.info(String.format("Dimension POST Status Code: %d", restClient.getResponseCode()));
                } catch(Exception ex) {
                    logger.log(Level.INFO, ex.getMessage(), ex);
                }
                batchCount = 1;
            }
        }
    }

    public void apply(ConcurrentHashMap<String, TagData> tagpaths) {
        int batchCount = 1;
        int totalCount = tagpaths.size();
        JSONArray dimensionObj = null;
        logger.info(String.format("Dimension Data: Tagpath count: %d", totalCount));
        for (Map.Entry<String, TagData> e : tagpaths.entrySet()) {
            if (batchCount == 1 ) dimensionObj = new JSONArray();

            TagData td = e.getValue();
            String id = td.getDeviceId();
            String tagpath = td.getTagpath();
            int dimensionStatus = td.getDimensionStatus();

            if (dimensionStatus == 0) {
                logger.fine("Add dimension: tagpath: " + tagpath + "    Dimention ID: " + id);
                dimensionObj.put(createDimItem(id, "CLIENT", "LITERAL", client));
                dimensionObj.put(createDimItem(id, "TAGPATH", "LITERAL", tagpath));
                batchCount += 2;
                td.setDimensionStatus(1);
    
                String[] tagLevels = tagpath.split("/");
                int levelCount = 0;
                for (String level : tagLevels) {
                    dimensionObj.put(createDimItem(id, "LEVEL_" + Integer.toString(levelCount) , "LITERAL", level));
                    levelCount += 1;
                    batchCount += 1;
                }
    
                totalCount = totalCount - 1;
     
                if (batchCount >= 80 || totalCount <= 1) {
                    try {
                        // invoke API to create dimensional data
                        logger.info("DimensionObj:   " + dimensionObj.toString());
                        restClient.post(dimensionalAPI, dimensionObj.toString());
                        logger.info(String.format("Dimension POST Status Code: %d", restClient.getResponseCode()));
                    } catch(Exception ex) {
                        logger.log(Level.INFO, ex.getMessage(), ex);
                    }
                    batchCount = 1;
                }
            } else {
                logger.fine("Dimension was already added: tagpath: " + tagpath + "    Dimention ID: " + id);
            }
        }
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

