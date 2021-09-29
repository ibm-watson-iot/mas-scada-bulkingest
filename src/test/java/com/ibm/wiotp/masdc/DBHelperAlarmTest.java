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

public class DBHelperAlarmTest {

    private Config config;

    private long currentTimeMilli = 0;
    private long currentTimeSecs = 0;
    private int  currentMonth = 0;
    private int  currentYear = 0;
    

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

        config = new Config(connConfig, "alarm");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        config.setInstallDir("./testdata");
        config.setDataDir("./testdata");

        System.out.println("StartDate: " + config.getStartDate());

        currentTimeMilli = System.currentTimeMillis();
        DateUtil duc = new DateUtil(currentTimeMilli);
        currentTimeSecs = duc.getTimeSecs();
        currentMonth = duc.getMonth();
        currentYear = duc.getYear();
    }      


    @Test
    public void testDBHelper_01() {

        System.out.println("");
        System.out.println("TEST_01: test Ignition SQL String - create new offset file");
        System.out.println("");

        // create DBHelper object
        DBHelper dbhelper = new DBHelper(config);

        // create a new offset file
        config.setStartDate("2021-09-15 00:00:00");
        OffsetRecord offRec = new OffsetRecord(config, true);
        assertNotNull("shouldn't be null", offRec);

        long startMilli = offRec.getStartTimeSecs() * 1000;
        long endMilli = offRec.getEndTimeSecs() * 1000;
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("offset: st=%d et=%d y=%d m=%d d=%d", startMilli, endMilli, year, month, day));

        String sqlStr = dbhelper.getIgnitionDBSql(startMilli, endMilli, year, month);

        System.out.println(String.format("SQLSTR: %s", sqlStr));

        assertEquals(2021, year);
        assertEquals(9, month);
        assertEquals(15, day);

    }

    @Test
    public void testDBHelper_02() {

        System.out.println("");
        System.out.println("TEST_02: test Ignition SQL String - use old offset file");
        System.out.println("");

        // create DBHelper object
        DBHelper dbhelper = new DBHelper(config);

        // use old offset file
        OffsetRecord offRec = new OffsetRecord(config, false);
        assertNotNull("shouldn't be null", offRec);

        long startMilli = offRec.getStartTimeSecs() * 1000;
        long endMilli = offRec.getEndTimeSecs() * 1000;
        int year = offRec.getYear();
        int month = offRec.getMonth();
        int day = offRec.getDay();

        System.out.println(String.format("offset: st=%d et=%d y=%d m=%d d=%d", startMilli, endMilli, year, month, day));

        String sqlStr = dbhelper.getIgnitionDBSql(startMilli, endMilli, year, month);

        System.out.println(String.format("SQLSTR: %s", sqlStr));

        assertEquals(2021, year);
        assertEquals(9, month);
        assertEquals(15, day);

        offRec.deleteOffsetFile();

    }
}

