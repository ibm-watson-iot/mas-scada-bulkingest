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

    private static long offsetInterval = 30L;
    private static long offsetIntervalHistorical = 1800L;
    private static int connectorType = 1; // 1-device, 2-alarm
    private static String startDate;
    private static String offsetFile;
    private static TimeZone localTZ = TimeZone.getDefault();
    private static Calendar cal = Calendar.getInstance();
    private static long startTimeSecs;
    private static long endTimeSecs;
    private static int  month;
    private static int  year;
    private static int  day;
    private static int status;
    private static AtomicLong processedCount = new AtomicLong(0);
    private static AtomicLong uploadedCount = new AtomicLong(0);
    private static AtomicLong rate = new AtomicLong(0);
    private static AtomicLong entityCount = new AtomicLong(0);
    private static AtomicLong entityTypeCount = new AtomicLong(0);
    private static int currTimeWindowCycle = 0;

    public OffsetRecord(Config config, boolean newOffsetFile) {
        this.connectorType = config.getConnectorType();
        this.startDate = config.getStartDate();

        String dataDir = config.getDataDir();
        String etype = config.getClientSite() + "_" + config.getConnectorTypeStr();

        if (dataDir.equals("")) {
            offsetFile = etype + ".offset";
        } else {
            offsetFile = config.getDataDir() + "/volume/data/" + etype + ".offset";
        }
        offsetInterval = config.getFetchInterval();
        if (offsetInterval > 120) offsetInterval = 120;
        offsetIntervalHistorical = config.getFetchIntervalHistorical();

        // check if offset file exists
        if (checkFileExists(offsetFile) == 1) {
            if (newOffsetFile) {
                logger.info("Create a new offset file");
                // delete existing offset file
                File f = new File(offsetFile);
                f.delete();
            }
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

    public int getDay() {
        return day;
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
        
    public long setEntityCount(long count) {
        return entityCount.addAndGet(count);
    }

    public long getEntityCount() {
        return entityCount.get();
    }
        
    public long setEntityTypeCount(long count) {
        return entityTypeCount.addAndGet(count);
    }

    public long getEntityTypeCount() {
        return entityTypeCount.get();
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

    public void deleteOffsetFile() {
        try {
            // delete existing offset file
            File f = new File(offsetFile);
            f.delete();
        } catch(Exception e) {}
    }
        
    public long getWaitTimeMilli(int waitFlag, long cycleStartTimeMillis) {
        long waitTime = 100;
        if (waitFlag == 1) {
            long cycleEndTimeMillis = System.currentTimeMillis();
            long timeDiff = (cycleEndTimeMillis - cycleStartTimeMillis) / 1000;
            if (timeDiff < offsetInterval) {
                waitTime = (offsetInterval - timeDiff) * 1000;
            }
        } else if (waitFlag == 2) {
            waitTime = offsetInterval * 1000;
        }
        return waitTime;
    }

    public int updateOffsetFile(long lastStartTimeSecs, long lastEndTimeSecs, int lastYear, int lastMonth, int status) {

        int retval = 0;

        long curTimeMillis = System.currentTimeMillis();  
        long curTimeSecs = curTimeMillis/1000;
        cal.setTimeInMillis(curTimeMillis);
        int curYear = cal.get(Calendar.YEAR);
        int curMonth = cal.get(Calendar.MONTH) + 1; // Calendar base MONTH is 0

        if (status == Constants.EXTRACT_STATUS_NO_TABLE) {

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
            updateOffsetByDate(startDate, Constants.EXTRACT_STATUS_INIT);
        }
    }

    private static int updateOffsetByDate(String dateStr, int status) {
        int retval = 1;
        JSONObject ofrec = new JSONObject();
        long currentTimeMilli = System.currentTimeMillis();
        DateUtil duc = new DateUtil(currentTimeMilli);

        DateUtil du = new DateUtil(dateStr);
        startTimeSecs = du.getTimeSecs();
        endTimeSecs = startTimeSecs + offsetInterval;
        month = du.getMonth();
        year = du.getYear();
        day = du.getDay();

        // compare dateStr with current date
        if (du.getTimeMilli() > duc.getTimeMilli()) {
            // dateStr is in future
            startTimeSecs = duc.getTimeSecs();
            endTimeSecs = startTimeSecs + offsetInterval;
            month = duc.getMonth();
            year = duc.getYear();
            day = duc.getDay();
            retval = 2;
        } else if (du.getTimeMilli() < duc.getTimeMilli()) {
            // dateStr is in past
            endTimeSecs = startTimeSecs + offsetIntervalHistorical;
            if (endTimeSecs > duc.getTimeSecs()) {
                endTimeSecs = duc.getTimeSecs();
            }
            retval = 0;
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

    private static int checkFileExists(String fileName) {
        int found = 0;
        try {
            File f = new File(fileName);
            if (f.exists()) {
                found = 1;
            }
        } catch (Exception e) {
            logger.info("File is not found. " + fileName + "  Msg: " + e.getMessage());
        }
        return found;
    }


}


