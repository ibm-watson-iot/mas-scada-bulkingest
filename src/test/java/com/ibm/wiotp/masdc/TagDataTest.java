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
import java.util.UUID;
import java.util.Set;
import java.util.Iterator;
import java.io.File;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class TagDataTest {

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
        deviceTypes.put("useDefaultDeviceType", 0);
        JSONArray patterns = new JSONArray();
        JSONObject typePattern = new JSONObject();
        typePattern.put("DefaultDeviceType", ".*");
        patterns.put(typePattern);
        deviceTypes.put("patterns", patterns);
        deviceTypes.put("groupBy", "patterns");
        JSONArray discardPatterns = new JSONArray();
        deviceTypes.put("discardPatterns", discardPatterns);
        connConfig.put("deviceTypes", deviceTypes);

        JSONObject alarmTypes = new JSONObject();
        alarmTypes.put("useDefaultAlarmType", 1);
        patterns = new JSONArray();
        JSONObject pattern = new JSONObject();
        pattern.put("DefaultAlarmType", ".*");
        patterns.put(pattern);
        alarmTypes.put("patterns", patterns);
        alarmTypes.put("groupBy", "patterns");
        deviceTypes.put("discardPatterns", discardPatterns);
        connConfig.put("alarmTypes", alarmTypes);

    }      


    private static int checkFileExists(String fileName) {
        int found = 0;
        try {
            File f = new File(fileName);
            if (f.exists()) {
                found = 1;
            }
        } catch (Exception e) {
            System.out.println("File is not found. " + fileName + "  Msg: " + e.getMessage());
        }
        return found;
    }

    @Test
    public void testAddTagData() {
        System.out.println("");
        System.out.println("TEST: Add TagData");
        System.out.println("");

        Config config = new Config(connConfig, "device");
        try {
            config.set();
        } catch(Exception e) {
            e.printStackTrace();
        }

        String clientSite = config.getClientSite();

        OffsetRecord offsetRecord = new OffsetRecord(config, false);
        String cacheName = clientSite + "_device_tags_v2";

        String cacheFilePathData = System.getProperty("user.dir") + "/tagcache/" + cacheName + ".data";
        String cacheFilePathKey = System.getProperty("user.dir") + "/tagcache/" + cacheName + ".key";

        System.out.println("cacheFilePathData = " + cacheFilePathData);

        if ( checkFileExists(cacheFilePathData) == 1) {
            // delete old cache file
            File f = new File(cacheFilePathData);
            f.delete();
            f = new File(cacheFilePathKey);
            f.delete();
        }

        CacheAccess<String, TagData> tagpaths = JCS.getInstance(cacheName);
        Set<String> tagList = tagpaths.getCacheControl().getKeySet();
        long totalDevices = tagList.size();
        System.out.println(String.format("Total tags in cache: %d", totalDevices));
        offsetRecord.setEntityCount(totalDevices);

        System.out.println("Connector Type: " + config.getConnectorTypeStr());

        String tag = "ca3/Boiler/temp";
        String tagpath = tag.toLowerCase();
        String idString = clientSite + ":" + tagpath;
        String dId = UUID.nameUUIDFromBytes(idString.getBytes()).toString();
        String dType = config.getTypeByTagname(tagpath);
        long   tid = 1001;
        TagData td = new TagData(clientSite, tagpath, dId, dType);
        try {
            tagpaths.putSafe(idString, td);
            offsetRecord.setEntityCount(1);
        } catch(Exception e) {}
        td.setId(tid);
        tagpaths.put(idString, td);

        System.out.println(String.format("SET tagpath: %s  dId: %s", idString, dId));
        TagData td1 = tagpaths.get(idString);
        String dId1 = td1.getDeviceId();
        tagpath = td1.getTagpath();
        String tagid = td.getId();
        System.out.println(String.format("GET tagpath: %s  dId: %s  tagid: %s", tagpath, dId1, tagid));

        assertEquals("Set/Get DeviceId", dId, dId1);

        for (int i=0; i<3; i++) {
            tag = "ca3/Boiler/temp";
            tagpath = tag.toLowerCase();
            idString = clientSite + ":" + tagpath;
            dId = UUID.nameUUIDFromBytes(idString.getBytes()).toString();
            dType = config.getTypeByTagname(tagpath);
            td = new TagData(clientSite, tagpath, dId, dType);
            tid = 1001;
            try {
                tagpaths.putSafe(idString, td);
                offsetRecord.setEntityCount(1);
                System.out.println(String.format("SET tagpath: %s  dId: %s", idString, dId));
            } catch(Exception e) {
                System.out.println("TAG Data exist in cache");
            }
            td.setId(tid);
            tagpaths.put(idString, td);
        }

        td1 = tagpaths.get(idString);
        dId1 = td1.getDeviceId();
        tagpath = td1.getTagpath();
        tagid = td.getId();
        System.out.println(String.format("GET tagpath: %s  dId: %s  tagid: %s", tagpath, dId1, tagid));

        for (int i=0; i<3; i++) {
            tag = "ca3/Boiler/temp/hi";
            tagpath = tag.toLowerCase();
            idString = clientSite + ":" + tagpath;
            dId = UUID.nameUUIDFromBytes(idString.getBytes()).toString();
            dType = config.getTypeByTagname(tagpath);
            td = new TagData(clientSite, tagpath, dId, dType);
            tid = 2222;
            try {
                tagpaths.putSafe(idString, td);
                offsetRecord.setEntityCount(1);
                System.out.println(String.format("SET tagpath: %s  dId: %s", idString, dId));
            } catch(Exception e) {
                System.out.println("TAG Data exist in cache");
            }
            td.setId(tid);
            tagpaths.put(idString, td);
        
            td1 = tagpaths.get(idString);
            dId1 = td1.getDeviceId();
            tagpath = td1.getTagpath();
            tagid = td.getId();
            System.out.println(String.format("GET tagpath: %s  dId: %s  tagid: %s", tagpath, dId1, tagid));
        }

        for (int i=0; i<3; i++) {
            tag = "ca3/Boiler/temp/hi";
            tagpath = tag.toLowerCase();
            idString = "TestSite2:" + tagpath;
            dId = UUID.nameUUIDFromBytes(idString.getBytes()).toString();
            dType = config.getTypeByTagname(tagpath);
            tid = 3322;
            td = new TagData("TestSite2", tagpath, dId, dType);
            try {
                tagpaths.putSafe(idString, td);
                offsetRecord.setEntityCount(1);
                System.out.println(String.format("SET tagpath: %s  dId: %s", idString, dId));
            } catch(Exception e) {
                System.out.println("TAG Data exist in cache");
            }
            td.setId(tid);
            tagpaths.put(idString, td);
        
            td1 = tagpaths.get(idString);
            dId1 = td1.getDeviceId();
            tagid = td.getId();
            System.out.println(String.format("GET tagpath: %s  dId: %s  tagid: %s", tagpath, dId1, tagid));
        }


        Set<String> tagList1 = tagpaths.getCacheControl().getKeySet();
        int totalDevicesInCache = tagList1.size();
        System.out.println(String.format("Total Devices: %d", totalDevicesInCache));

        Iterator<String> it = tagList1.iterator();
        while (it.hasNext()) {
            String id = it.next();
            TagData td2 = tagpaths.get(id);
            if (td2 == null) {
                System.out.println("TD IS NULL");
                continue;
            }
            String deviceId = td2.getDeviceId();
            String deviceType = td2.getDeviceType();
            String tag1 = td2.getTagpath();
            String site = td2.getSiteName();
            String tid1 = td2.getId();

            System.out.println(String.format("Site:%s Tag:%s dId:%s tagId:%s Type:%s", site, tag1, deviceId, tid1, deviceType));
        }



        JCS.shutdown();

    }

}

