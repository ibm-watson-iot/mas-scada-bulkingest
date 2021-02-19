/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.*;


public class DateUtil {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private TimeZone localTZ = TimeZone.getDefault();
    private Calendar cal;
    private long timeMilli;
    private long timeSecs;
    private int  month;
    private int  year;
    private int  day;


    public DateUtil() {
    }

    public DateUtil(String dateStr) {
        setByDate(dateStr);
    }

    public DateUtil(long timeMilli) {
        setByMilliseconds(timeMilli);
    }

    public void setByDate(String dateStr) {
        timeMilli = 0;
        timeSecs = 0;
        month = 0;
        year = 0;
        day = 0;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(dateStr);
            timeMilli = date.getTime();
            timeSecs = timeMilli/1000;
            cal = Calendar.getInstance(localTZ);
            cal.setTime(date);
            month = cal.get(Calendar.MONTH) + 1;
            year = cal.get(Calendar.YEAR);
        } catch(Exception ex) {
            logger.log(Level.INFO, "Failed to set time values: ", ex);
        }
    }

    public void setByMilliseconds(long tmMilli) {
        timeMilli = tmMilli;
        timeSecs = tmMilli/1000;
        month = 0;
        year = 0;
        try {
            Date date = new Date(tmMilli);
            cal = Calendar.getInstance(localTZ);
            cal.setTime(date);
            month = cal.get(Calendar.MONTH) + 1;
            year = cal.get(Calendar.YEAR);
        } catch(Exception ex) {
            logger.log(Level.INFO, "Failed to set time values: ", ex);
        }
    }

    public long getTimeMilli() {
        return timeMilli;
    }

    public long getTimeSecs() {
        return timeSecs;
    }

    public int getMonth() {
        return month;
    }

    public int getYear() {
        return year;
    }

}


