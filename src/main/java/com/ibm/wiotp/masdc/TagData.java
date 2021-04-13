/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.io.Serializable;

public class TagData implements Serializable {

    private String tagpath;
    private String deviceId;
    private String deviceType;
    private int deviceStatus = 0;    // 0-NotCreatedYet 1-Created
    private int dimensionStatus = 0; // 0-NotAddedYet 1-Added

    public TagData(String tagpath, String deviceId) {
        this.tagpath = tagpath;
        this.deviceId = deviceId;
    }

    public TagData(String tagpath, String deviceId, String deviceType) {
        this.tagpath = tagpath;
        this.deviceId = deviceId;
        this.deviceType = deviceType;
    }

    public String getTagpath() {
        return this.tagpath;
    }

    public String getDeviceId() {
        return this.deviceId;
    }

    public String getDeviceType() {
        return this.deviceType;
    }

    public int getDeviceStatus() {
        return this.deviceStatus;
    }

    public void setDeviceStatus(int status) {
        this.deviceStatus = status;
    }

    public int getDimensionStatus() {
        return this.dimensionStatus;
    }

    public void setDimensionStatus(int status) {
        this.dimensionStatus = status;
    }
}


