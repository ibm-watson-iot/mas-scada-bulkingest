/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.logging.*;
import org.json.JSONObject;
import org.json.JSONArray;

public class EntityType {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config;
    private static String type;
    private static String geo;
    private static String tenantId;
    private static String entityTypeName;
    private static String schemaName;
    private static String metricTableName;
    private static String dimensionTableName;
    private static String baseUrl;
    private static JSONObject wiotp;
    private static JSONObject datalake;
    private static JSONArray entityObj = null;
    private static RestClient restClient;
    private static String entityAPI;

    public EntityType(Config config) throws Exception {
        if (config == null) {
            throw new NullPointerException("config/tagpaths parameter cannot be null");
        }

        this.type = config.getConnectorTypeStr();
        this.wiotp = config.getWiotpConfig();
        this.datalake = config.getMonitorConfig();

        this.geo = wiotp.getString("geo"); 
        this.baseUrl = "https://api-" + geo + ".connectedproducts.internetofthings.ibmcloud.com/api";
        this.tenantId = wiotp.getString("tenantId");
        this.entityTypeName = config.getEntityType();
        this.schemaName = datalake.getString("schema");
        this.metricTableName = "IOT_" + entityTypeName;
        this.dimensionTableName = metricTableName + "_CTG";

        this.entityAPI = "/meta/v1/" + this.tenantId + "/entityType";
        this.restClient = new RestClient(baseUrl, 2, wiotp.getString("key"), wiotp.getString("token"), config.getPostResponseFile());
    }

    public void register() throws Exception {

        if (entityObj == null ) {
            entityObj = new JSONArray();

            JSONObject entityTypeObj = new JSONObject();
            entityTypeObj.put("name", entityTypeName);
            entityTypeObj.put("description", entityTypeName);
            entityTypeObj.put("metricTableName", metricTableName);
            entityTypeObj.put("dimensionTableName", dimensionTableName);
            entityTypeObj.put("metricTimestampColumn", "RCV_TIMESTAMP_UTC");
            entityTypeObj.put("schemaName", schemaName);
        
            JSONArray dataItemDtoArray = new JSONArray();
        
            // Add metric objects in dataItemDtoArray
            dataItemDtoArray.put(createDataDtoObject("RCV_TIMESTAMP_UTC", "METRIC", "RCV_TIMESTAMP_UTC", "TIMESTAMP"));
            dataItemDtoArray.put(createDataDtoObject("entity_id", "METRIC", "DEVICEID", "LITERAL"));
            if (type.equals("device")) {
                dataItemDtoArray.put(createDataDtoObject("tagid", "METRIC", "TAGID", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("intvalue", "METRIC", "INTVALUE", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("floatvalue", "METRIC", "FLOATVALUE", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("stringvalue", "METRIC", "STRINGVALUE", "LITERAL"));
                dataItemDtoArray.put(createDataDtoObject("datevalue", "METRIC", "DATEVALUE", "LITERAL"));
                dataItemDtoArray.put(createDataDtoObject("evt_name", "METRIC", "EVT_NAME", "LITERAL"));
            } else {
                dataItemDtoArray.put(createDataDtoObject("name", "METRIC", "NAME", "LITERAL"));
                dataItemDtoArray.put(createDataDtoObject("alarmid", "METRIC", "ALARMID", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("etype", "METRIC", "ETYPE", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("eventid", "METRIC", "EVENTID", "LITERAL"));
                dataItemDtoArray.put(createDataDtoObject("displaypath", "METRIC", "DISPLAYPATH", "LITERAL"));
                dataItemDtoArray.put(createDataDtoObject("value", "METRIC", "VALUE", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("priority", "METRIC", "PRIORITY", "NUMBER"));
                dataItemDtoArray.put(createDataDtoObject("ackby", "METRIC", "ACKBY", "LITERAL"));
            }
        
            // Add Dimension objects in dataItemDtoArray
            dataItemDtoArray.put(createDataDtoObject("CLIENT",  "DIMENSION", "CLIENT", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("TAGPATH", "DIMENSION", "TAGPATH", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_0", "DIMENSION", "LEVEL_0", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_1", "DIMENSION", "LEVEL_1", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_2", "DIMENSION", "LEVEL_2", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_3", "DIMENSION", "LEVEL_3", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_4", "DIMENSION", "LEVEL_4", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_5", "DIMENSION", "LEVEL_5", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_6", "DIMENSION", "LEVEL_6", "LITERAL"));
            dataItemDtoArray.put(createDataDtoObject("LEVEL_7", "DIMENSION", "LEVEL_7", "LITERAL"));
            
            // Add dataItemDtoArray in entityTypeObj
            entityTypeObj.put("dataItemDto", dataItemDtoArray);
            entityObj.put(entityTypeObj);

            restClient.post(entityAPI, entityObj.toString());
            logger.info(String.format("EntityType POST Status Code: %d", restClient.getResponseCode()));
        }
    }

    public String getEndpoint() {
        String entityTypeEndpoint = baseUrl + "/meta/v1/" + this.tenantId + "/entityType";
        return entityTypeEndpoint;
    }

    private static JSONObject createDataDtoObject(String name, String type, String colName, String colType) {
        JSONObject dtoObj = new JSONObject();
        dtoObj.put("name", name);
        dtoObj.put("type", type);
        dtoObj.put("columnName", colName);
        dtoObj.put("columnType", colType);
        return dtoObj;
    }
}


