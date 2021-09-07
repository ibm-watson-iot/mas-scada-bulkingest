package com.ibm.wiotp.masdc;

import org.junit.rules.ExpectedException;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.json.JSONObject;
import org.json.JSONArray;

public class ParseTagnameTestPatterns {

    private JSONObject connConfig = new JSONObject();

    @Before
    public void beforeAll() {
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
        JSONObject typePatternx = new JSONObject();
        typePatternx.put("TestType01", "ca3/hvac/.*zonetemp");
        patterns.put(typePatternx);
        JSONObject typePatterny = new JSONObject();
        typePatterny.put("TestType01", "ca3/hvac/.*roompressure");
        patterns.put(typePatterny);
        JSONObject typePattern1 = new JSONObject();
        typePattern1.put("TestType01", "ca3/hvac/.*roomtempf");
        patterns.put(typePattern1);
        deviceTypes.put("patterns", patterns);
        deviceTypes.put("groupBy", "patterns");
        JSONArray discardPatterns = new JSONArray();
        deviceTypes.put("discardPatterns", discardPatterns);
        connConfig.put("deviceTypes", deviceTypes);

        JSONObject alarmTypes = new JSONObject();
        patterns = new JSONArray();
        JSONObject pattern = new JSONObject();
        pattern.put("DefaultAlarmType", ".*");
        patterns.put(pattern);
        alarmTypes.put("patterns", patterns);
        alarmTypes.put("groupBy", "patterns");
        alarmTypes.put("discardPatterns", discardPatterns);
        connConfig.put("alarmTypes", alarmTypes);

        // System.out.println(connConfig.toString(4));
    }      


    @Test
    public void testSameDeviceType() {
        System.out.println("");
        System.out.println("TEST: SameDeviceType ParseTagname");
        System.out.println("");

        Config config = new Config(connConfig, "device");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Connector Type: " + config.getConnectorTypeStr());

        String type = config.getTypeByTagname("ca3/hvac/tcp 7/rfu1_11zonetemp");
        System.out.println("Device Type for ca3/hvac/tcp 7/rfu1_11zonetemp: " + type);
        assertEquals("ca3/hvac/tcp 7/rfu1_11zonetemp : TestType01", "TestType01", type);
        type = config.getTypeByTagname("ca3/hvac/lab153/cu3/fc3_10/roomtempf");
        assertEquals("ca3/hvac/lab153/cu3/fc3_10/roomtempf : TestType01", "TestType01", type);
        type = config.getTypeByTagname("ca3/hvac/quad4/cleanroom/roompressure");
        assertEquals("ca3/hvac/quad4/cleanroom/roompressure : TestType01", "TestType01", type);
        System.out.println("Device Type for ca3/hvac/quad4/cleanroom/roompressure: " + type);
        type = config.getTypeByTagname("ca3/xhvac/quad4/cleanroom/roompressure");
        assertNotEquals("ca3/xhvac/quad4/cleanroom/roompressure : TestType01", "TestType01", type);
        type = config.getTypeByTagname("ca3/hvac/tcp 7/rfu1_11zonetemp");
        System.out.println("Device Type for ca3/hvac/tcp 7/rfu1_11zonetemp: " + type);
        assertEquals("ca3/hvac/tcp 7/rfu1_11zonetemp : TestType01", "TestType01", type);
    }

}

