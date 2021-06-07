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
import java.util.ListIterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class Dimension {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config;
    private static CacheAccess<String, TagData> tagpaths;
    private static String client;
    private static String connectorTypeStr;
    private static String name;
    private static List<String> entityTypes;
    private static JSONObject wiotp;
    private static String baseUrl;
    private static String statsDimAPI;
    private static HashMap<String, String> dimAPIMap = new HashMap<String, String>();
    private static RestClient restClient;
    private static String token;

    public Dimension(Config config, CacheAccess<String, TagData> tagpaths) {
        if (config == null) {
            throw new NullPointerException("config parameter cannot be null");
        }

        this.config = config;
        this.tagpaths = tagpaths;

        this.client = config.getClientSite();
        this.connectorTypeStr = config.getConnectorTypeStr();
        this.name = config.getEntityType();
        this.entityTypes = config.getTypes();
        this.wiotp = config.getWiotpConfig();
        this.token = wiotp.getString("token");
        this.baseUrl = "https://api-" + wiotp.getString("geo") + ".connectedproducts.internetofthings.ibmcloud.com/api";
        this.statsDimAPI = "/master/v1/" + wiotp.getString("tenantId") + "/entityType/" +
            config.getStatsDeviceType() + "/dimensional";

        ListIterator<String> itr = null;
        itr = entityTypes.listIterator();
        while (itr.hasNext()) {
            String eType = itr.next();
            String dAPI = "/master/v1/" + wiotp.getString("tenantId") + "/entityType/" + eType + "/dimensional";
            dimAPIMap.put(eType, dAPI);
        }
            
        this.restClient = new RestClient(baseUrl, 2, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void start() {
        startDimensionThread();
    }

    public void addStatsDimensions() {
        JSONArray dimensionObj = new JSONArray();
        String deviceId = config.getStatsDeviceId();
        String deviceType = config.getStatsDeviceType();
        logger.info(String.format("Add dimension data: Id:%s Type:%s Client:%s", deviceId, deviceType, client));

        dimensionObj.put(createDimItem(deviceId, "CLIENT", "LITERAL", client));
        dimensionObj.put(createDimItem(deviceId, "CONNECTOR", "LITERAL", deviceType));
 
        try {
            logger.fine("DimensionObj: " + dimensionObj.toString());
            restClient.post(statsDimAPI, dimensionObj.toString());
            logger.info(String.format("Dimension POST Status Code: %d", restClient.getResponseCode()));
        } catch(Exception ex) {
            logger.log(Level.FINE, ex.getMessage(), ex);
        }
    }
    

    private static void processDimensions() {
        int dimAdded = 0;
        int dimRegistered = 0;
        int batchCount = 0;
        JSONArray dimensionObj = null;
        Set<String> tagList = tagpaths.getCacheControl().getKeySet();
        int totalCountInCache = tagList.size();
        int totalCount = totalCountInCache;

        logger.info(String.format("Dimension Data: Tagpath InCache:%d", totalCountInCache));

        boolean done = true;
        List<String> doneTags = new ArrayList<String>();
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
            String deviceType = td.getDeviceType();
            String dimApi = dimAPIMap.get(deviceType);
            String tagpath = td.getTagpath();
            String tid = td.getId();

            int dimensionStatus = td.getDimensionStatus();

            if (dimensionStatus == 0) {
                logger.info(String.format("Add dimension: tagpath:%s Type:%s Id:%s TId:%s", tagpath, deviceType, deviceId, tid));
                dimensionObj.put(createDimItem(deviceId, "CLIENT", "LITERAL", client));
                dimensionObj.put(createDimItem(deviceId, "TAGPATH", "LITERAL", tagpath));
                batchCount += 2;
                if (connectorTypeStr.equals("device")) {
                    dimensionObj.put(createDimItem(deviceId, "TID", "LITERAL", tid));
                    batchCount += 1;
                }
                td.setDimensionStatus(1);
                tagpaths.put(id, td);
                doneTags.add(id);
    
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
 
            // if (batchCount >= 2 || totalCount == 1) {
            if (dimensionStatus == 0) {
                for (int retry=0; retry<5; retry++) {
                    done = true;
                    if (batchCount == 1) break;
                    try {
                        // invoke API to create dimensional data
                        logger.fine("DimensionObj:   " + dimensionObj.toString());
                        restClient.post(dimApi, dimensionObj.toString());
                        logger.info(String.format("Dimension POST Status Code: %d", restClient.getResponseCode()));
                    } catch(Exception ex) {
                        logger.info("Exception message: " + ex.getMessage());
                        logger.log(Level.FINE, ex.getMessage(), ex);
                        done = false;
                    }
                    if (done) break;
                    try {
                        Thread.sleep(5000);
                    } catch(Exception e) {}
                    logger.info(String.format("Retry REST call: retry count=%d", retry)); 
                }
                batchCount = 0;
            }
            if (done == false) break;
            totalCount = totalCount - 1;
        }

        if (done == false) {
            // cycle didn't complete successfully, reset dimension state so that these can be retried in the next cycle
            logger.info("Could not register dimensions of all tags in the cycle. Backing out status of unregistered tags.");
            Iterator<String> doneTagList = doneTags.iterator();
            while (doneTagList.hasNext()) {
                String id = doneTagList.next();
                TagData td = tagpaths.get(id);
                td.setDimensionStatus(0);
                dimRegistered -= 1;
            }
        }
    
        logger.info(String.format("Total=%d New=%d AlreadyRegistered=%d", totalCountInCache, dimAdded, dimRegistered));
    }

    private static JSONObject createDimItem(String id, String name, String type, String value) {
        JSONObject dimItem = new JSONObject();
        dimItem.put("id", id);
        dimItem.put("name", name);
        dimItem.put("type", type);
        dimItem.put("value", value);
        return dimItem;
    }

    private static void startDimensionThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Start add dimension data cycle");
                    if (config.getUpdateFlag() == 1) break;
                    processDimensions();
                    try {
                        Thread.sleep(15000);
                    } catch (Exception e) {}
                }
            }
        };
        logger.info("Starting dimension registration thread ...");
        new Thread(thread).start();
        logger.info("Dimension registration thread is started");
        return;
    }

}

