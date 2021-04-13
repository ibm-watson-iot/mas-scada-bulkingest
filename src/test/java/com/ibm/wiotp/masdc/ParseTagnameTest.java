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

public class ParseTagnameTest {

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
        typePatternx.put("TestType01", "oakville/pump/.*");
        patterns.put(typePatternx);
        JSONObject typePatterny = new JSONObject();
        typePatterny.put("TestType02", "rutherford/compressor/.*");
        patterns.put(typePatterny);
        JSONObject typePattern1 = new JSONObject();
        typePattern1.put("CA3DeviceType", "ca3/.*");
        patterns.put(typePattern1);
        JSONObject typePattern2 = new JSONObject();
        typePattern2.put("CA3EDeviceType", "ca3e/.*");
        patterns.put(typePattern2);
        JSONObject typePattern3 = new JSONObject();
        typePattern3.put("CA5DeviceType", "ca5/.*");
        patterns.put(typePattern3);
        JSONObject typePattern4 = new JSONObject();
        typePattern4.put("DefaultDeviceType", ".*");
        patterns.put(typePattern4);
        deviceTypes.put("patterns", patterns);
        deviceTypes.put("groupBy", "patterns");
        connConfig.put("deviceTypes", deviceTypes);

        JSONObject alarmTypes = new JSONObject();
        patterns = new JSONArray();
        JSONObject pattern = new JSONObject();
        pattern.put("DefaultAlarmType", ".*");
        patterns.put(pattern);
        alarmTypes.put("patterns", patterns);
        alarmTypes.put("groupBy", "patterns");
        connConfig.put("alarmTypes", alarmTypes);

        // System.out.println(connConfig.toString(4));
    }      


    @Test
    public void testDeviceType() {
        System.out.println("");
        System.out.println("TEST: DeviceType ParseTagname");
        System.out.println("");

        Config config = new Config(connConfig, "device");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Connector Type: " + config.getConnectorTypeStr());

        String type = config.getTypeByTagname("ca3/boiler/temp");
        assertEquals("ca3/boiler/temp : CA3DeviceType", "CA3DeviceType", type);
        type = config.getTypeByTagname("oakville/pump/xxx");
        assertEquals("oakville/pump/xxx : TestType01", "TestType01", type);
        type = config.getTypeByTagname("rutherford/compressor/xxx");
        assertEquals("rutherford/compressor : TestType02", "TestType02", type);
        type = config.getTypeByTagname("ca3e/compressor/pressure");
        assertEquals("ca3e/compressore/pressure : CA3EDeviceType", "CA3EDeviceType", type);
        type = config.getTypeByTagname("ca3eboiler/temp");
        assertEquals("ca3eboiler/temp : DefaultDeviceType", "DefaultDeviceType", type);
        type = config.getTypeByTagname("ca3");
        assertEquals("ca3 : DefaultDeviceType", "DefaultDeviceType", type);
        type = config.getTypeByTagname("ca5");
        assertNotEquals("ca5 : CA5DeviceType", "CA5DeviceType", type);
    }

    @Test
    public void testAlarmType() {
        System.out.println("");
        System.out.println("TEST: AlarmType ParseTagname");
        System.out.println("");

        Config config = new Config(connConfig, "alarm");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Connector Type: " + config.getConnectorTypeStr());

        String type = config.getTypeByTagname("ca3/boiler/temp");
        assertEquals("ca3/boiler/temp : DefaultAlarmType", "DefaultAlarmType", type);
        type = config.getTypeByTagname("ca3e/compressor/pressure");
        assertEquals("ca3e/compressore/pressure : DefaultAlarmType", "DefaultAlarmType", type);
        type = config.getTypeByTagname("ca5");
        assertNotEquals("ca5 : CA5DeviceType", "CA5DeviceType", type);
    }
}

