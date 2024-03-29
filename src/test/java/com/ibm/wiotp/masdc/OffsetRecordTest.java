package com.ibm.wiotp.masdc;

import org.junit.rules.ExpectedException;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.json.JSONObject;
import org.json.JSONArray;

public class OffsetRecordTest {

    private Config config;

    private long currentTimeMilli = 0;
    private long currentTimeSecs = 0;
    private int  currentMonth = 0;
    private int  currentYear = 0;
    private long lastTestTimeMilli = 0;
    private long lastTestTimeSecs = 0;
    private int  lastTestMonth = 0;
    private int  lastTestYear = 0;
    

    @Before
    public void beforeAll() {
        JSONObject connConfig = new JSONObject();
        connConfig.put("clientSite", "testSite");
        connConfig.put("runMode", 0);
        connConfig.put("fetchInterval", 30L);
        connConfig.put("fetchIntervalHistorical", 14400L);
        connConfig.put("startDate", "2020-05-01 00:00:00");

        JSONObject wiotp = new JSONObject();
        wiotp.put("orgId", "xxxxxx");
        wiotp.put("key", "a-xxxxxx-889xn93it8");
        wiotp.put("token" , "xxxx-V4LmHP-N?xxxx");
        wiotp.put("tenantId", "beta-2");
        wiotp.put("geo", "beta");
        connConfig.put("wiotp", wiotp);

        JSONObject monitor = new JSONObject();
        monitor.put("dbtype", "db2");
        monitor.put("host", "db2w-xxxxxxx.us-south.db2w.cloud.ibm.com");
        monitor.put("port", "50001");
        monitor.put("schema", "BLUADMIN");
        monitor.put("user", "bluadmin");
        monitor.put("password", "xxxx4FXmd_eFE47gsI8Ttcx2Jxxxx");
        connConfig.put("monitor", monitor);

        JSONObject ignition = new JSONObject();
        ignition.put("dbtype", "mysql");
        ignition.put("host", "127.0.0.1");
        ignition.put("port", "3306");
        ignition.put("schema", "scadadata");
        ignition.put("database", "scadadata");
        ignition.put("user", "root");
        ignition.put("password", "xxxxxxxx");
        connConfig.put("ignition", ignition); 

        JSONObject deviceTypes = new JSONObject();
        JSONArray patterns = new JSONArray();
        JSONObject pattern = new JSONObject();
        pattern.put("DefaultDeviceType", ".*");
        patterns.put(pattern);
        deviceTypes.put("patterns", patterns);
        deviceTypes.put("groupBy", "patterns");
        JSONArray discardPatterns = new JSONArray();
        deviceTypes.put("discardPatterns", discardPatterns);
        connConfig.put("deviceTypes", deviceTypes);

        JSONObject alarmTypes = new JSONObject();
        patterns = new JSONArray();
        pattern = new JSONObject();
        pattern.put("DefaultAlarmType", ".*");
        patterns.put(pattern);
        alarmTypes.put("patterns", patterns);
        alarmTypes.put("groupBy", "patterns");
        deviceTypes.put("discardPatterns", discardPatterns);
        connConfig.put("alarmTypes", alarmTypes);

        // System.out.println(connConfig.toString(4));

        config = new Config(connConfig, "device");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("StartDate: " + config.getStartDate());

        currentTimeMilli = System.currentTimeMillis();
        DateUtil duc = new DateUtil(currentTimeMilli);
        currentTimeSecs = duc.getTimeSecs();
        currentMonth = duc.getMonth();
        currentYear = duc.getYear();
        lastTestYear = currentYear;
        lastTestMonth = currentMonth;
    }      


    @Test
    public void testOffsetRecord_01() {

        System.out.println("");
        System.out.println("TEST_01: testOffsetRecordNoTable");
        System.out.println("");

        // create a new offset file
        OffsetRecord offRec = new OffsetRecord(config, true);
        assertNotNull("shouldn't be null", offRec);

        // config start date is 2020-05-01 00:00:00
        DateUtil du = new DateUtil("2020-05-01 00:00:00");
        long tms = du.getTimeSecs();
        long tme = tms + config.getFetchIntervalHistorical();

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("y=%d m=%d d=%d st=%d et=%d", year, month, day, startTimeSecs, endTimeSecs));

        assertEquals(tms, startTimeSecs);
        assertEquals(tme, endTimeSecs);
        assertEquals(2020, year);
        assertEquals(5, month);

        // with no table, month is incremented
        for (int i=6; i<22; i++) {
            int nowait = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_NO_TABLE);
            String newDateStr;
            int newYear = 2020;
            int j = i;
            if (i>=13) {
                j = i - 12;
                newYear = 2021;
                newDateStr = String.format("2021-%02d-01 00:00:00", j);
            } else {
                newDateStr = String.format("2020-%02d-01 00:00:00", j);
            }
            // System.out.println("New Date: " + newDateStr);
            du = new DateUtil(newDateStr);
            tms = du.getTimeSecs();
            tme = tms + config.getFetchIntervalHistorical();

            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();

            System.out.println(String.format("y=%d m=%d d=%d st=%d et=%d", year, month, day, startTimeSecs, endTimeSecs));

            if (year != currentYear && month != currentMonth) {
                assertEquals(tms, startTimeSecs);
                assertEquals(tme, endTimeSecs);
                assertEquals(newYear, year);
                assertEquals(j, month);
            } else {
                try {
                    Thread.sleep(1000);
                } catch(Exception e) {}
            }
        }

    }

    @Test
    public void testOffsetRecord_02() {

        System.out.println("");
        System.out.println("TEST_02: testOffsetRecordNoTableOldRecFile");
        System.out.println("");

        // read existing offset file
        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("y=%d m=%d d=%d st=%d et=%d", year, month, day, startTimeSecs, endTimeSecs));

        assertEquals(currentYear, year);
        assertEquals(currentMonth, month);

    }

    @Test
    public void testOffsetRecord_03() {

        System.out.println("");
        System.out.println("TEST_03: testOffsetRecordTableNoData");
        System.out.println("");

        // set new startDate
        config.setStartDate("2021-01-01 00:00:00");
        config.setFetchIntervalHistorical(86400);

        // create a new offset file
        OffsetRecord offRec = new OffsetRecord(config, true);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        long waitTime = 0;
        long cycleStartTimeMillis = 0;
        int waitFlag = 0;
        for (int i=1; i<32; i++) {
            cycleStartTimeMillis = System.currentTimeMillis();
            System.out.println(String.format("loop=%d y=%d m=%d d=%d st=%d et=%d wf=%d", i, year, month, day, startTimeSecs, endTimeSecs, waitFlag));
            assertEquals(year, 2021);
            assertEquals(month, 1);
            assertEquals(day, i);
            assertEquals(waitFlag, 0);

            waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_NO_DATA);
            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();

            try {
                Thread.sleep(1500);
            } catch(Exception e) {}
            waitTime = offRec.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
            assertEquals(waitTime, 100);
        }

        waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_NO_DATA);
        assertEquals(year, 2021);
        assertEquals(month, 2);
        assertEquals(day, 1);

        offRec.deleteOffsetFile();
    }

    @Test
    public void testOffsetRecord_04() {

        System.out.println("");
        System.out.println("TEST_04: testOffsetRecordTableWithData");
        System.out.println("");

        // set startDate
        config.setStartDate("2021-01-01 00:00:00");
        config.setFetchIntervalHistorical(86400);

        // check and create new offset file
        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("StartingRecord: %d %d", startTimeSecs, endTimeSecs));
        long waitTime = 0;
        long cycleStartTimeMillis;
        for (int i=0; i<30; i++) {
            cycleStartTimeMillis = System.currentTimeMillis();
            int waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();

            System.out.println(String.format("loop=%d y=%d m=%d d=%d st=%d et=%d wf=%d", i, year, month, day, startTimeSecs, endTimeSecs, waitFlag));
        
            assertEquals(waitFlag, 0);
            try {
                Thread.sleep(1500);
            } catch(Exception e) {}
            waitTime = offRec.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
            assertEquals(waitTime, 100);
        }

        for (int i=0; i<1; i++) {
            cycleStartTimeMillis = System.currentTimeMillis();
            int waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();
            
            System.out.println(String.format("loop=%d y=%d m=%d d=%d st=%d et=%d wf=%d", i, year, month, day, startTimeSecs, endTimeSecs, waitFlag));

            if ( endTimeSecs < (cycleStartTimeMillis/1000)) {
                assertEquals(waitFlag, 0);
            } else {
                assertEquals(waitFlag, 1);
            }
            try {
                Thread.sleep(1500);
            } catch(Exception e) {}
            waitTime = offRec.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
            System.out.println(String.format("WaitTime: %d", waitTime));
        }

        offRec.deleteOffsetFile();
    }

    @Test
    public void testOffsetRecord_05() {

        System.out.println("");
        System.out.println("TEST_05: testOffsetRecordTableWithCurrentDateNoData");
        System.out.println("");

        // set startDate
        config.setStartDate("2021-09-15 00:00:00");
        config.setFetchIntervalHistorical(86400);

        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("StartingRecord: %d %d", startTimeSecs, endTimeSecs));

        int we = 0;
        long waitTime = 0;
        long cycleStartTimeMillis;
        for (int i=0; i<10; i++) {
            cycleStartTimeMillis = System.currentTimeMillis();
            int waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();
 
            System.out.println(String.format("loop=%d y=%d m=%d d=%d st=%d et=%d wf=%d", i, year, month, day, startTimeSecs, endTimeSecs, waitFlag));
            assertEquals(waitFlag, 0);
            try {
                Thread.sleep(1500);
            } catch(Exception e) {}
            waitTime = offRec.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
            assertEquals(waitTime, 100);
        }

        for (int i=0; i<1; i++) {
            cycleStartTimeMillis = System.currentTimeMillis();
            int waitFlag = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, Constants.EXTRACT_STATUS_TABLE_WITH_DATA);
            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            System.out.println(String.format("StartingRecord: %d %d", startTimeSecs, endTimeSecs));

            year = offRec.getYear();
            month = offRec.getMonth();
            day = offRec.getDay();
        
            lastTestYear = year;
            lastTestMonth = month;
            
            System.out.println(String.format("loop=%d y=%d m=%d d=%d st=%d et=%d wf=%d", i, year, month, day, startTimeSecs, endTimeSecs, waitFlag));
            
            if ( endTimeSecs < (cycleStartTimeMillis/1000)) {
                assertEquals(waitFlag, 0);
            } else {
                assertEquals(waitFlag, 1);
            }
            try {
                Thread.sleep(1500);
            } catch(Exception e) {}
            waitTime = offRec.getWaitTimeMilli(waitFlag, cycleStartTimeMillis);
            System.out.println(String.format("WaitTime: %d", waitTime));
        }


        // Read last test file and validate the data
        System.out.println("");
        System.out.println("TEST_05A: testOffsetRecordTableWithCurrentDateNoData");
        System.out.println("");

        // read existing offset file
        offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        startTimeSecs = offRec.getStartTimeSecs();
        endTimeSecs = offRec.getEndTimeSecs();
        year = offRec.getYear();
        month = offRec.getMonth();
        day = offRec.getDay();

        System.out.println(String.format("y=%d m=%d d=%d st=%d et=%d", year, month, day, startTimeSecs, endTimeSecs));

        assertEquals(lastTestYear, year);
        assertEquals(lastTestMonth, month);

        offRec.deleteOffsetFile();
    }

}

