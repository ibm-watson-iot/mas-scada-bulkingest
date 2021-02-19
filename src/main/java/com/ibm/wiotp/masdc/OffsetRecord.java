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
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicLong;


public class OffsetRecord {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    public static final int STATUS_INIT = 0;
    public static final int STATUS_TABLE_WITH_DATA = 1;
    public static final int STATUS_TABLE_NO_DATA = 2;
    public static final int STATUS_NO_TABLE = 3;
    public static long offsetInterval = 30L;
    public static long offsetIntervalHistorical = 1800L;

    private static int connectorType = 1; // 1-device, 2-alarm
    private static String startDate;
    private static String offsetFile;

    private static TimeZone localTZ = TimeZone.getDefault();
    private static Calendar cal = Calendar.getInstance();
    private static long startTimeSecs;
    private static long endTimeSecs;
    private static int  month;
    private static int  year;
    private static int status;
    private static AtomicLong processedCount = new AtomicLong(0);
    private static AtomicLong uploadedCount = new AtomicLong(0);
    private static AtomicLong rate = new AtomicLong(0);

    public OffsetRecord(Config config, boolean newOffsetFile) {
        this.connectorType = config.getConnectorType();
        this.startDate = config.getStartDate();

        String dataDir = config.getDataDir();
        if (dataDir.equals("")) {
            offsetFile = config.getEntityType() + ".offset";
        } else {
            offsetFile = config.getDataDir() + "/volume/config/" + config.getEntityType() + ".offset";
        }
        offsetInterval = config.getFetchInterval();
        offsetIntervalHistorical = config.getFetchIntervalHistorical();

        if (newOffsetFile) {
            // delete existing offset file
            File f = new File(offsetFile);
            f.delete();
        }

        readOffsetFile();
        logger.info(String.format("StartDate:%s StartTime:%d EndTime:%d", startDate, startTimeSecs, endTimeSecs));
    }

    public long getStartTimeSecs() {
        return startTimeSecs;
    }
        
    public long getEndTimeSecs() {
        return endTimeSecs;
    }
        
    public int getMonth() {
        return month;
    }
        
    public int getYear() {
        return year;
    }

    public long setProcessedCount(long count) {
        return processedCount.addAndGet(count);
    }

    public long getProcessedCount() {
        return processedCount.get();
    }
        
    public long setUploadedCount(long count) {
        return uploadedCount.addAndGet(count);
    }

    public long getUploadedCount() {
        return uploadedCount.get();
    }
        
    public void setRate(long count) {
        if (count == 0) return;
        rate.set(count);
    }

    public long getRate() {
        return rate.get();
    }

    public String getOffsetFile() {
        return offsetFile;
    }
        
    public int updateOffsetFile(long lastStartTimeSecs, long lastEndTimeSecs, int lastYear, int lastMonth, int status) {

        int retval = 0;

        long curTimeMillis = System.currentTimeMillis();  
        long curTimeSecs = curTimeMillis/1000;
        cal.setTimeInMillis(curTimeMillis);
        int curYear = cal.get(Calendar.YEAR);
        int curMonth = cal.get(Calendar.MONTH) + 1; // Calendar base MONTH is 0

        if (status == STATUS_NO_TABLE) {

            int nextYear = lastYear;
            int nextMonth = lastMonth + 1;
            if (nextMonth > 13) {
                nextMonth = 1;
                nextYear = nextYear + 1;
                if (nextYear > curYear) {
                    nextYear = curYear;
                    nextMonth = curMonth;
                }
            }

            if (lastYear > curYear) {
                // start date in config is a future date or incorrect system date. can not proceed
                logger.severe("Future start date in config or incorrect system time. Can not proceed.");
                logger.info("Reset start date to current date");
                nextYear = curYear;
                nextMonth = curMonth;
            }

            String newDateStr = String.format("%4d-%02d-01 00:00:00", nextYear, nextMonth);
            retval = updateOffsetByDate(newDateStr, status);

        } else {

            Date date = new Date(lastEndTimeSecs * 1000);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = df.format(date);
            retval = updateOffsetByDate(dateStr, status);

        }

        return retval;
    }

    private void readOffsetFile() {
        int createFile = 0;
        JSONObject ofrec = null;
        try {
            String offsetRecordStr = new String (Files.readAllBytes(Paths.get(offsetFile)));
            ofrec = new JSONObject(offsetRecordStr);
        }
        catch (Exception e) {
            logger.info("Data offset file is not found. " + e.getMessage());
            createFile = 1;
        }

        if (ofrec != null) {
            // update data 
            startTimeSecs = ofrec.getLong("startTimeSecs");
            if (startTimeSecs != 0) {
                endTimeSecs = ofrec.getLong("endTimeSecs");
                month = ofrec.getInt("month");
                year = ofrec.getInt("year");
                status = ofrec.getInt("status");
                processedCount.set(ofrec.getInt("processed"));
                uploadedCount.set(ofrec.getInt("uploaded"));
                rate.set(ofrec.getInt("rate"));
            } else {
                createFile = 1;
            }
        }

        if (createFile == 1 || ofrec == null) {
            updateOffsetByDate(startDate, STATUS_INIT);
        }
    }

    private static int updateOffsetByDate(String dateStr, int status) {
        int retval = 0;
        JSONObject ofrec = new JSONObject();
        long currentTimeMilli = System.currentTimeMillis();
        DateUtil duc = new DateUtil(currentTimeMilli);

        DateUtil du = new DateUtil(dateStr);
        startTimeSecs = du.getTimeSecs();
        endTimeSecs = startTimeSecs + offsetInterval;
        month = du.getMonth();
        year = du.getYear();

        if (startTimeSecs > duc.getTimeSecs()) {
            // configured time is in future - log and set to current time
            logger.info("Configured start time is in future. Starting from current system time.");
            startTimeSecs = duc.getTimeSecs();
            endTimeSecs = startTimeSecs + offsetInterval;
            month = duc.getMonth();
            year = duc.getYear();
        } else {
            if (year < duc.getYear()) {
                endTimeSecs = startTimeSecs + offsetIntervalHistorical;
                retval = 1;
            } else if (month < duc.getMonth()) {
                endTimeSecs = startTimeSecs + offsetIntervalHistorical;
                retval = 1;
            }
        }

        ofrec.put("startTimeSecs", startTimeSecs);
        ofrec.put("endTimeSecs", endTimeSecs);
        ofrec.put("month", month);
        ofrec.put("year", year);
        ofrec.put("status", status);
        ofrec.put("processed", processedCount.get());
        ofrec.put("uploaded", uploadedCount.get());
        ofrec.put("rate", rate.get());
        writeOffsetFile(ofrec.toString());
        return retval;
    }


    private static void writeOffsetFile(String offsetRec) {
        try {
            FileWriter fw = new FileWriter(offsetFile);
            fw.write(offsetRec);
            fw.flush();
            fw.close();
        } catch (Exception ex) {
            logger.log(Level.INFO, "Failed to update offset file: ", ex);
        }
    }

}


