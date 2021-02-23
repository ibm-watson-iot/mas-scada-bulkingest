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
    private String token;

    public Device(Config config, String client, String type, String name, JSONObject wiotp) {
        this.config = config;
        this.client = client;
        this.type = type;
        this.name = name;
        this.wiotp = wiotp;
        this.token = wiotp.getString("token");
        this.baseUrl = "https://" + wiotp.getString("orgId") + ".internetofthings.ibmcloud.com/";
        this.deviceAPI = "api/v0002/bulk/devices/add";
        this.restClient = new RestClient(baseUrl, 1, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void apply(CacheAccess<String, TagData> tagpaths) {
        int devicesRegistered = 0;
        int alreadyRegistered = 0;
        int batchCount = 0;
        JSONArray deviceObj = null;
        int totalDevices = tagpaths.getCacheControl().getSize();
        logger.info(String.format("Device ID count: %d", totalDevices));
        Set<String> tagList = tagpaths.getCacheControl().getKeySet();
        Iterator<String> it = tagList.iterator();
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
            int deviceStatus = td.getDeviceStatus();

            if (deviceStatus == 0) {
                logger.fine("Add device: " + deviceId);
                deviceObj.put(createDeviceItem(name, deviceId, token));
                td.setDeviceStatus(1);
                batchCount += 1;
                devicesRegistered += 1;
            } else {
                logger.fine("Device is already registered: id: " + deviceId);
                alreadyRegistered += 1;
            }

            if (batchCount >= 100 || totalDevices == 1) {
                try {
                    restClient.post(deviceAPI, deviceObj.toString());
                    logger.info(String.format("Add Device POST Status Code: %d", restClient.getResponseCode()));
                } catch(Exception ex) {
                    logger.log(Level.INFO, ex.getMessage(), ex);
                }
                batchCount = 0;
            }
            totalDevices = totalDevices - 1;
        }
        logger.info(String.format("Total=%d New:%d AlreadyRegistered:%d", totalDevices, devicesRegistered, alreadyRegistered));
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

