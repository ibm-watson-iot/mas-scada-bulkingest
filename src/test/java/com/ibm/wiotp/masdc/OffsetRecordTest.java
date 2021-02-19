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

public class OffsetRecordTest {

    private Config config;

    private long currentTimeMilli = 0;
    private long currentTimeSecs = 0;
    private int  currentMonth = 0;
    private int  currentYear = 0;
    

    @Before
    public void beforeAll() {
        JSONObject connConfig = new JSONObject();
        connConfig.put("clientSite", "testSite");
        connConfig.put("deviceType", "TestDeviceType");
        connConfig.put("alarmType", "TestAlarmType");
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
        wiotp.put("dbtype", "db2");
        wiotp.put("host", "db2w-xxxxxxx.us-south.db2w.cloud.ibm.com");
        wiotp.put("port", "50001");
        wiotp.put("schema", "BLUADMIN");
        wiotp.put("user", "bluadmin");
        wiotp.put("password", "xxxx4FXmd_eFE47gsI8Ttcx2Jxxxx");
        connConfig.put("monitor", monitor);

        JSONObject ignition = new JSONObject();
        wiotp.put("dbtype", "mysql");
        wiotp.put("host", "127.0.0.1");
        wiotp.put("port", "3306");
        wiotp.put("schema", "scadadata");
        wiotp.put("database", "scadadata");
        wiotp.put("user", "root");
        wiotp.put("password", "xxxxxxxx");
        connConfig.put("ignition", ignition); 

        config = new Config(connConfig);
        try {
            config.set();
        } catch(Exception e) {}

        System.out.println("StartDate: " + config.getStartDate());

        currentTimeMilli = System.currentTimeMillis();
        DateUtil duc = new DateUtil(currentTimeMilli);
        currentTimeSecs = duc.getTimeSecs();
        currentMonth = duc.getMonth();
        currentYear = duc.getYear();
    }      


    @Test
    public void testOffsetRecord_01() {

        System.out.println("");
        System.out.println("TEST_01: testOffsetRecordNoTable");
        System.out.println("");

        OffsetRecord offRec = new OffsetRecord(config, true);
        assertNotNull("shouldn't be null", offRec);

        DateUtil du = new DateUtil("2020-05-01 00:00:00");
        long tms = du.getTimeSecs();
        long tme = tms + config.getFetchIntervalHistorical();

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();

        System.out.println(String.format("StartingRecord: %d %d   %d %d", tms, startTimeSecs, tme, endTimeSecs));

        assertEquals(tms, startTimeSecs);
        assertEquals(tme, endTimeSecs);
        assertEquals(2020, year);
        assertEquals(5, month);

        for (int i=6; i<18; i++) {
            int nowait = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, offRec.STATUS_NO_TABLE);
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
            System.out.println("New Date: " + newDateStr);
            du = new DateUtil(newDateStr);
            tms = du.getTimeSecs();
            tme = tms + config.getFetchIntervalHistorical();

            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();

            System.out.println(String.format("OffsetRec: %d %d %d %d - %d", startTimeSecs, endTimeSecs, year, month, nowait));

            if (year != currentYear && month != currentMonth) {
                assertEquals(tms, startTimeSecs);
                assertEquals(tme, endTimeSecs);
                assertEquals(newYear, year);
                assertEquals(j, month);
            } else {
                try {
                    Thread.sleep(2000);
                } catch(Exception e) {}
            }
        }

    }

    @Test
    public void testOffsetRecord_02() {

        System.out.println("");
        System.out.println("TEST_02: testOffsetRecordNoTableOldRecFile");
        System.out.println("");

        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();

        System.out.println(String.format("StartingRecord: %d %d   %d %d", startTimeSecs, endTimeSecs, year, month));

        assertEquals(currentYear, year);
        assertEquals(currentMonth, month);

    }

    @Test
    public void testOffsetRecord_03() {

        System.out.println("");
        System.out.println("TEST_03: testOffsetRecordTableNoData");
        System.out.println("");

        // set startDate
        config.setStartDate("2021-01-01 00:00:00");
        config.setFetchIntervalHistorical(86400);

        OffsetRecord offRec = new OffsetRecord(config, true);
        assertNotNull("shouldn't be null", offRec);

        DateUtil du = new DateUtil("2021-01-01 00:00:00");
        long tms = du.getTimeSecs();
        long tme = tms + config.getFetchIntervalHistorical();

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();

        System.out.println(String.format("StartingRecord: %d=%d   %d=%d", tms, startTimeSecs, tme, endTimeSecs));

        assertEquals(tms, startTimeSecs);
        assertEquals(tme, endTimeSecs);
        assertEquals(2021, year);
        assertEquals(1, month);

        for (int i=0; i<35; i++) {
            int nowait = offRec.updateOffsetFile(startTimeSecs, endTimeSecs, year, month, offRec.STATUS_TABLE_NO_DATA);

            startTimeSecs = offRec.getStartTimeSecs();
            endTimeSecs = offRec.getEndTimeSecs();
            year = offRec.getYear();
            month = offRec.getMonth();

            Date date = new Date(startTimeSecs * 1000);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = df.format(date);

            System.out.println(String.format("OffsetRec: %d %d %d %d - %d -- %s", startTimeSecs, endTimeSecs, year, month, nowait, dateStr));

            if (month != currentMonth) {
                tme = startTimeSecs + config.getFetchIntervalHistorical();
                assertEquals(tme, endTimeSecs);
            } else {
                tme = startTimeSecs + config.getFetchInterval();
                assertEquals(tme, endTimeSecs);
            }
        }

    }

    @Test
    public void testOffsetRecord_04() {

        System.out.println("");
        System.out.println("TEST_04: testOffsetRecordNoDataOldRecFile");
        System.out.println("");

        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startTimeSecs = offRec.getStartTimeSecs();
        long endTimeSecs = offRec.getEndTimeSecs();
        int year = offRec.getYear();
        int month = offRec.getMonth();

        System.out.println(String.format("StartingRecord: %d %d   %d %d", startTimeSecs, endTimeSecs, year, month));

        assertEquals(currentYear, year);
        assertEquals(currentMonth, month);

        // remove temp offset file
        offRec.deleteOffsetFile();

    }

}

