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
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class Device {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config;
    private static CacheAccess<String, TagData> tagpaths;
    private static String client;
    private static String type;
    private static String name;
    private static JSONObject wiotp;
    private static String baseUrl;
    private static String deviceAPI;
    private static RestClient restClient;
    private static String token;

    public Device(Config config, CacheAccess<String, TagData> tagpaths) {
        if (config == null || tagpaths == null) {
            throw new NullPointerException("config/tagpaths parameter cannot be null");
        }

        this.config = config;
        this.tagpaths = tagpaths;

        this.client = config.getClientSite();
        this.type = config.getConnectorTypeStr();
        this.name = config.getEntityType();
        this.wiotp = config.getWiotpConfig();
        this.token = wiotp.getString("token");
        this.baseUrl = "https://" + wiotp.getString("orgId") + ".internetofthings.ibmcloud.com/";
        this.deviceAPI = "api/v0002/bulk/devices/add";
        this.restClient = new RestClient(baseUrl, 1, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void start() {
        startDeviceThread();
    }

    private static void processDevices() {
        int devicesRegistered = 0;
        int alreadyRegistered = 0;
        int batchCount = 0;
        JSONArray deviceObj = null;
        Set<String> tagList = tagpaths.getCacheControl().getKeySet();
        int totalDevicesInCache = tagList.size();
        int totalDevices = totalDevicesInCache;
        logger.info(String.format("Devices: InCache:%d NoKeys:%d", totalDevicesInCache, totalDevices));
        
        boolean done = true;
        Iterator<String> it = tagList.iterator();
        List<String> doneTags = new ArrayList<String>();
        while (it.hasNext()) {
            if (batchCount == 0 ) {
                deviceObj = new JSONArray();
                batchCount = 1;
            }

            String id = it.next();
            TagData td = tagpaths.get(id);
            if (td == null) {
                totalDevices = totalDevices - 1;
                continue;
            }
            String deviceId = td.getDeviceId();
            String deviceType = td.getDeviceType();
            int deviceStatus = td.getDeviceStatus();

            if (deviceStatus == 0) {
                logger.info("Add device: " + deviceId + "  type: " + deviceType);
                deviceObj.put(createDeviceItem(deviceType, deviceId, token));
                td.setDeviceStatus(1);
                tagpaths.put(id, td);
                batchCount += 1;
                devicesRegistered += 1;
                doneTags.add(id);
            } else {
                logger.fine("Device is already registered: id: " + deviceId);
                alreadyRegistered += 1;
            }

            if (batchCount >= 100 || totalDevices == 1) {
                for (int retry=0; retry<5; retry++) {
                    done = true;
                    if (batchCount == 1) break;
                    try {
                        restClient.post(deviceAPI, deviceObj.toString());
                        logger.info(String.format("Add Device POST Status Code: %d", restClient.getResponseCode()));
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
            totalDevices = totalDevices - 1;
        }

        if (done == false) {
            // cycle didn't complete successfully, reset device state so that these can be retried in the next cycle
            logger.info("Could not register all devices in the cycle. Backing out status of unregistered devices.");
            Iterator<String> doneTagList = doneTags.iterator();
            while (doneTagList.hasNext()) {
                String id = doneTagList.next();
                TagData td = tagpaths.get(id);
                td.setDeviceStatus(0);
                devicesRegistered -= 1;
            }
        }
            
        logger.info(String.format("Total=%d New=%d AlreadyRegistered=%d", totalDevicesInCache, devicesRegistered, alreadyRegistered));
    }

    private static JSONObject createDeviceItem(String typeId, String deviceId, String token) {
        JSONObject devItem = new JSONObject();
        devItem.put("typeId", typeId);
        devItem.put("deviceId", deviceId);
        devItem.put("authToken", token);
        JSONObject deviceInfo = new JSONObject();
        JSONObject location = new JSONObject();
        JSONObject metadata = new JSONObject();
        devItem.put("deviceInfo", deviceInfo);
        devItem.put("location", location);
        devItem.put("metadata", metadata);
        return devItem;
    }

    // Thread to create devices
     private static void startDeviceThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Start add devices cycle");
                    if (config.getUpdateFlag() == 1) break;
                    processDevices();
                    try {
                        Thread.sleep(15000);
                    } catch (Exception e) {}
                }
            }
        };
        logger.info("Starting device registration thread ...");
        new Thread(thread).start();
        logger.info("Device registration thread is started");
        return;
    }
}

