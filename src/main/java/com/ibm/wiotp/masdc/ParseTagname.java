/*
 *  Copyright (c) 2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ParseTagname {
    private static final Logger logger = Logger.getLogger("mas-ignition-connector");
  
    int tagMapCount = 0;
    String defaultType = "";
    int useDefaultType = 1;
    List<Pattern> patterns = new ArrayList<>();
    List<String> entityTypes = new ArrayList<String>();
    List<Pattern> discardPatterns = new ArrayList<>();
    int discardTagMapCount = 0;

    public ParseTagname(Config config) {
        JSONArray types;
        JSONArray discardTypes;
        int useDefaultType = 1;
        if (config.getConnectorType() == Constants.CONNECTOR_DEVICE) {
            types = config.getDeviceTypes();
            useDefaultType = config.getUseDefaultDeviceType();
            discardTypes = config.getDiscardDeviceTypes();
        } else {
            types = config.getAlarmTypes();
            useDefaultType = config.getUseDefaultAlarmType();
            discardTypes = config.getDiscardAlarmTypes();
        }

        tagMapCount = types.length();
        discardTagMapCount = discardTypes.length();

        logger.info(String.format("tagMapCount:%d useDefaultType:%d discardTagMapCount:%d", tagMapCount, useDefaultType, discardTagMapCount));

        for (int i = 0; i < tagMapCount; i++) {
            JSONObject obj = types.getJSONObject(i);
            Iterator<String> keys = obj.keys();
            String typeStr = keys.next();
            patterns.add(i, Pattern.compile(obj.optString(typeStr)));
            entityTypes.add(i, typeStr);
            if (i == tagMapCount-1) {
                if (useDefaultType == 1) {
                    defaultType = typeStr;
                }
            }
        }

        for (int i = 0; i < discardTagMapCount; i++) {
            String discardTypeStr = discardTypes.getString(i);
            // System.out.println("=====> discardTypeStr: " + discardTypeStr);
            discardPatterns.add(i, Pattern.compile(discardTypeStr));
        }

    }

    public String getType(String tagName) {
        int index = 0;
        String type = "";

        for (Pattern pattern: patterns) {
            Matcher m = pattern.matcher(tagName);
            boolean matches = m.matches();
            if (matches == true) {
                // check if tagName is in discard list
                int discard = 0;
                if (discardTagMapCount > 0) {
                    for (Pattern discardPattern: discardPatterns) {
                        Matcher md = discardPattern.matcher(tagName);
                        boolean mdmatches = md.matches();
                        if (mdmatches == true) {
                            logger.info("======> Tag discarded : " + tagName);
                            discard = 1;
                            break;
                        }
                    }
                }
                if (discard == 1) continue;
                type = entityTypes.get(index);
                // System.out.println("tagName:" + tagName +"   Type: " + type);
                break;
            }
            index += 1;
        }
        return type;
    }

    public String getDefaultType() {
        return defaultType;
    }

    public List<String> getTypes() {
        return entityTypes;
    }

}

