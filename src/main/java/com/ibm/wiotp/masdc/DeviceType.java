/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.List;
import java.util.ListIterator;
import java.util.logging.*;
import org.json.JSONObject;
import org.json.JSONArray;

public class DeviceType {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static String client;
    private static int    type;
    private static List<String> entityTypes;
    // private static String name;
    private static JSONObject wiotp;
    private static String baseUrl;
    private static String deviceTypeAPI;
    private static RestClient restClient;
    private static Config config;

    public DeviceType(Config config) throws Exception {
        if (config == null) {
            throw new NullPointerException("config/tagpaths parameter cannot be null");
        }

        this.config = config;

        this.client = config.getClientSite();
        this.type = config.getConnectorType();
        // this.name = config.getEntityType();
        this.entityTypes = config.getTypes();
        this.wiotp = config.getWiotpConfig();
        this.baseUrl = "https://" + wiotp.getString("orgId") + ".internetofthings.ibmcloud.com/";
        this.deviceTypeAPI = "api/v0002/device/types";
        this.restClient = new RestClient(baseUrl, 1, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void register() {
        ListIterator<String> itr = null;
        itr = entityTypes.listIterator();
        while (itr.hasNext()) {
            String name = itr.next();
            JSONObject deviceObj = createDeviceTypeItem(name);
            try {
                restClient.post(deviceTypeAPI, deviceObj.toString());
                logger.info(String.format("Add DeviceType:%s  POST Status Code: %d", name, restClient.getResponseCode()));
            } catch(Exception ex) {
                logger.log(Level.INFO, ex.getMessage(), ex);
            }
        }

        // create stats device type
        String statsDeviceType = config.getStatsDeviceType();
        JSONObject statsDeviceObj = createDeviceTypeItem(statsDeviceType);
        try {
            restClient.post(deviceTypeAPI, statsDeviceObj.toString());
            logger.info(String.format("Add Stats DeviceType: %s  POST Status Code: %d", statsDeviceType, restClient.getResponseCode()));
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

