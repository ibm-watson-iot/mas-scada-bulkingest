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

public class Device {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private String client;
    private String type;
    private String name;
    private JSONObject wiotp;
    private String baseUrl;
    private String deviceAPI;
    private RestClient restClient;
    private Config config;

    public Device(Config config, String client, String type, String name, JSONObject wiotp) {
        this.config = config;
        this.client = client;
        this.type = type;
        this.name = name;
        this.wiotp = wiotp;
        this.baseUrl = "https://" + wiotp.getString("orgId") + ".internetofthings.ibmcloud.com/";
        this.deviceAPI = "api/v0002/bulk/devices/add";
        this.restClient = new RestClient(baseUrl, 1, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void apply(HashMap<String, String> tagpaths) {
        int batchCount = 1;
        JSONArray deviceObj = null;
        int totalDevices = tagpaths.size();
        logger.info(String.format("Device ID count: %d", totalDevices));
        for (String tagpath : tagpaths.keySet()) {
            if (batchCount == 1 ) deviceObj = new JSONArray();
            String id = tagpaths.get(tagpath);
            deviceObj.put(createDeviceItem(name, id, wiotp.getString("token")));
            batchCount += 1;
            totalDevices = totalDevices - 1;
            if (batchCount >= 100 || totalDevices == 1) {
                try {
                    restClient.post(deviceAPI, deviceObj.toString());
                    logger.info(String.format("Add Device POST Status Code: %d", restClient.getResponseCode()));
                } catch(Exception ex) {
                    logger.log(Level.INFO, ex.getMessage(), ex);
                }
                batchCount = 1;
            }
        }
    }

    public void apply(ConcurrentHashMap<String, TagData> tagpaths) {
        int batchCount = 1;
        JSONArray deviceObj = null;
        int totalDevices = tagpaths.size();
        logger.info(String.format("Device ID count: %d", totalDevices));
        for (Map.Entry<String, TagData> e : tagpaths.entrySet()) {
            if (batchCount == 1 ) deviceObj = new JSONArray();
            TagData td = e.getValue();
            String id = td.getDeviceId();
            int deviceStatus = td.getDeviceStatus();
            if (deviceStatus == 0) {
                logger.fine("Add device: " + id);
                deviceObj.put(createDeviceItem(name, id, wiotp.getString("token")));
                batchCount += 1;
                totalDevices = totalDevices - 1;
                td.setDeviceStatus(1);
                if (batchCount >= 100 || totalDevices == 1) {
                    try {
                        restClient.post(deviceAPI, deviceObj.toString());
                        logger.info(String.format("Add Device POST Status Code: %d", restClient.getResponseCode()));
                    } catch(Exception ex) {
                        logger.log(Level.INFO, ex.getMessage(), ex);
                    }
                    batchCount = 1;
                }
            } else {
                logger.fine("Already s already registered: id: " + id);
            }
        }
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

}

