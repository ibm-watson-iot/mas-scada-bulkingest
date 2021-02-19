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
import java.util.logging.*;
import org.json.JSONObject;
import org.json.JSONArray;

public class DeviceType {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private String client;
    private String type;
    private String name;
    private JSONObject wiotp;
    private String baseUrl;
    private String deviceTypeAPI;
    private RestClient restClient;
    private Config config;

    public DeviceType(Config config, String client, String type, String name, JSONObject wiotp) {
        this.config = config;
        this.client = client;
        this.type = type;
        this.name = name;
        this.wiotp = wiotp;
        this.baseUrl = "https://" + wiotp.getString("orgId") + ".internetofthings.ibmcloud.com/";
        this.deviceTypeAPI = "api/v0002/device/types";
        this.restClient = new RestClient(baseUrl, 1, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void apply() {
        int batchCount = 1;
        JSONObject deviceObj = createDeviceTypeItem(name);
        try {
            restClient.post(deviceTypeAPI, deviceObj.toString());
            logger.info(String.format("Add DeviceType POST Status Code: %d", restClient.getResponseCode()));
        } catch(Exception ex) {
            logger.log(Level.INFO, ex.getMessage(), ex);
        }
    }

    private static JSONObject createDeviceTypeItem(String typeId) {
        JSONObject devItem = new JSONObject();
        devItem.put("id", typeId);
        devItem.put("description", typeId);
        devItem.put("classId", "Device");
        JSONObject deviceInfo = new JSONObject();
        JSONObject metadata = new JSONObject();
        devItem.put("deviceInfo", deviceInfo);
        devItem.put("metadata", metadata);
        return devItem;
    }
}

