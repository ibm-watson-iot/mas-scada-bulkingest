/*
 *  Copyright (c) 2019-2020 IBM Corporation and other Contributors.
 * 
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */


package com.ibm.wiotp.masdc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.sql.*;
import java.sql.Types.*;
import java.nio.file.*;
import java.util.logging.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.json.*;

// Extract data from SCADA historian database (mySQL and MSSQL), dump in a csv file,
// and execute script to process data

public class DBConnector {

    // Variables
    static final int MAX_TABLE_COLUMNS = 50;

    static String connConfigFile = "";
    static String tableConfigFile = "";
    static String tableRunStatusFile = "";
    static String uploadStatsFile = "";
    static String sourceHost = "";
    static String sourcePort = "";
    static String sourceDatabase = "";
    static String sourceSchema = "";
    static String sourceUser = "";
    static String sourcePassword = "";
    static String dbType = "";
    static String csvFilePath = "";
    static String prcFilePath = "";
    static String offFilePath = "";
    static String clnFilePath = "";
    static String smpFilePath = "";
    static String prCsvFilePath = "";
    static String ddlFilePath = "";
    static String pythonPath = "";
    static String userHome = "";
    static String tableName = "";
    static String tstampColName = "";
    static String installDir = "";
    static String dataDir = "";
    static String logFile = "";
    static String insertSQL = "";
    static String sortStr = "";
    static int chunkSize = 50000;
    static int sampleChunkSize = 5;
    static int scanInterval = 120;
    static int batchInsertSize = 10000;
    static boolean formatSqlStatement = false;
    static String customSql = "";
    static String customSqlFile = "";
    static int sampleEventCount = 1;
    static Logger logger = Logger.getLogger("dataingest.extract");  
    static FileHandler fh;  
    static JSONArray collist = new JSONArray();
    static String [] cname = new String[MAX_TABLE_COLUMNS];
    static String [] ctype = new String[MAX_TABLE_COLUMNS];
    static FileWriter fwStats;
    static String pType = "";
    static int runMode = 0;
    static int colsProcessed = 0;
    

    /**
     * @param tableName      Name of the table to extract data from. This is a required item.
     */
    public static void main(String[] args) {

        String extractSqlFile = "";

        try {
            tableName = args[0];

            // Validate arguments
            if (tableName.isEmpty()) {
                logger.info("ExtractData: Required argument tableName is not specified.");
                System.exit(1);
            }

            // Check if only db extraction is needed
            if (args.length > 1) {
                extractSqlFile = args[1];
            }

            // Get user home dir
            userHome = System.getProperty("user.home");

            // Set simpleFormatter format
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT : %4$s : %2$s : %5$s%6$s%n");

            // Get install and data dir location from enviironment variables
            Map <String, String> map = System.getenv();
            for ( Map.Entry <String, String> entry: map.entrySet() ) {
                if ( entry.getKey().compareTo("IBM_DATAINGEST_INSTALL_DIR") == 0 ) {
                    installDir = entry.getValue();
                } else if ( entry.getKey().compareTo("IBM_DATAINGEST_DATA_DIR") == 0 ) {
                    dataDir = entry.getValue();
                }
            }
            if ( installDir.compareTo("") == 0 ) {
                installDir = userHome + "/ibm/masdc";
            } 
            if ( dataDir.compareTo("") == 0 ) {
                dataDir = userHome + "/ibm/masdc";
            } 

            // set log file handler
            Handler[] handlers = logger.getHandlers(); 
            System.out.printf("Handlers %d\n", handlers.length);
            for ( int i = 0; i < handlers.length; i++ ) {
                System.out.printf("Handler %d\n", i);
                Handler lh = handlers[i];
                logger.removeHandler(lh);
            }

            if ( extractSqlFile.compareTo("") == 0 ) {
                logFile = installDir + "/volume/logs/" + tableName + "/connector.log";
            } else {
                logFile = installDir + "/volume/logs/extract_" + extractSqlFile.replace(".sql", ".log");
            }
            fh = new FileHandler(logFile);  
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            logger.info("Data processing: " + tableName);

            String osname = System.getProperty("os.name");
            if ( osname.startsWith("Windows")) {
                pythonPath = installDir + "/python-3.7.5/python.exe";
            } else {
                pythonPath = "python3";
            }

            // Set configuration file paths
            connConfigFile = dataDir + "/volume/config/connection.json";
            tableConfigFile = dataDir + "/volume/config/" + tableName + ".json";
            tableRunStatusFile = dataDir + "/volume/config/" + tableName + ".running";
            offFilePath = dataDir + "/volume/config/" + tableName + ".offset";
            uploadStatsFile = dataDir + "/volume/data/" + tableName + "/data/" + tableName + "_uploadStats.csv";

            // Get SCADA historian database configuration
            logger.info("Read connection configuration file: " + connConfigFile);
            String fileContent = new String(Files.readAllBytes(Paths.get(connConfigFile)));
            JSONObject connConfig = new JSONObject(fileContent);
            JSONObject scada = connConfig.getJSONObject("scada");
            JSONObject datalake = connConfig.getJSONObject("datalake");
            dbType = scada.getString("dbtype");

            // Check if historian is not configured and csv files are created using other options
            if ( dbType.compareTo("none") != 0 ) {
                sourceHost = scada.getString("host");
                sourcePort = scada.getString("port");
                sourceDatabase = scada.getString("database");
                sourceSchema = scada.getString("schema");
                sourceUser = scada.getString("user");
                sourcePassword = scada.getString("password");
            }

            // Check if only data extraction is needed using an sql statement
            if (extractSqlFile.compareTo("") != 0) {
                extractOnly(extractSqlFile);
                return;
            } 

            // Get table configuration
            fileContent = new String(Files.readAllBytes(Paths.get(tableConfigFile)));
            logger.info("Read device type configuration file: " + tableConfigFile);
            JSONObject tableConfig = new JSONObject(fileContent);
            JSONObject dbConfig = tableConfig.getJSONObject("database");
            JSONObject eventData = tableConfig.getJSONObject("eventData");
            chunkSize = dbConfig.getInt("fetchSize");
            sampleChunkSize = dbConfig.optInt("sampleFetchSize", 5);
            scanInterval = dbConfig.getInt("scanInterval");
            formatSqlStatement = dbConfig.getBoolean("formatSqlStatement");
            customSqlFile = dbConfig.getString("sqlFile");
            sampleEventCount = tableConfig.getInt("mqttEvents");
            batchInsertSize = dbConfig.getInt("insertSize");
            tstampColName = eventData.getString("timestamp");
            pType = tableConfig.getString("type");

            // check for run mode - 0=production, 1=extractOnly, 2=testMode
            runMode = tableConfig.getInt("runMode");
            String msg = String.format("Run mode is %d\n", runMode);
            logger.info(msg);

            // Read customSql statement from file, if file is specified
            String custSqlFilePath;
            if (formatSqlStatement == true) {
                custSqlFilePath = dataDir + "/volume/data/" + tableName + "/data/" + tableName + ".sql";
            } else {
                custSqlFilePath = dataDir + "/volume/config/" + customSqlFile;
            }
            customSql = new String(Files.readAllBytes(Paths.get(custSqlFilePath)));

           
            // data files 
            csvFilePath = dataDir + "/volume/data/csv/" + tableName + ".csv";
            prcFilePath = dataDir + "/volume/data/" + tableName + "/data/.processed";
            clnFilePath = dataDir + "/volume/data/" + tableName + "/schemas/" + tableName + ".dcols";
            smpFilePath = dataDir + "/volume/data/" + tableName + "/schemas/.sampleEventSent";
            prCsvFilePath = dataDir + "/volume/data/" + tableName + "/data/" + tableName + ".csv";
            ddlFilePath = dataDir + "/volume/data/" + tableName + "/schemas/" + tableName + ".ddl";

            // Open stats file for accounting
            // A CSV file with the following data
            // LogTime, Extract Size,Columns,Rows, Upload Size,Columns,Rows, UploadStatus, TSLastRecord 

            //
            // If a new file, add header
            File tmpUploadFile = new File(uploadStatsFile);
            if ( tmpUploadFile.exists()) {
                logger.info("Open an existing upload stats file");
                fwStats = new FileWriter(uploadStatsFile, true);
            } else {
                logger.info("Open a new upload stats file");
                fwStats = new FileWriter(uploadStatsFile, true);
                String statsHeader = String.format("logTime,extSize,extCols,extRows,upSize,upCols,upRows,uploaded,tsLastRec\n");
                fwStats.write(statsHeader);
                fwStats.flush();

            }

            // Extract sample data, identify entiries and register with WIoTP
            // skip if smpFilePath exist
            File smpSendFile = new File(smpFilePath);
            if ( smpSendFile.exists()) {
                logger.info("Skip registration - sample send file exists.");
            } else {
                register();
            }

            // Extract data, and upload to data lake
            if ( runMode == 0 || runMode == 3 ) {
                upload(datalake);
            }

            logger.info("End: Data processing: " + tableName);

            // delete running status file
            try {         
                File f= new File(tableRunStatusFile);
                if (f.delete()) {  
                    logger.info("Deleted run status file: " + tableName);
                } else {  
                    logger.info("Could not delete run status file: " + tableName);
                }  
            } catch(Exception e) {  
                e.printStackTrace();  
            }  

            // close stats file
            fwStats.close();

        } catch (Exception ex) {
            logger.info("Exception information: " + ex.getMessage());
            ex.printStackTrace();
        }
        logger.info("Exit and wait for next scan cycle to start.");
    }


    // Extract sample data and configure/register WIoTP
    private static void register() {

        FileWriter fw;
        Connection conn = null;
        Statement stmt = null;

        try {
            // Check for script action
            boolean useChunk = false;
            String scriptPath = installDir + "/bin/register.py";

            // DB connection
            logger.info("Connecting to database to extract data from " + tableName);
            boolean getNextChunk = true;
            int type = 1;
            String DB_URL = "jdbc:sqlserver://"+sourceHost+":"+sourcePort+";databaseName="+sourceDatabase+";user="+
                sourceUser+";password="+sourcePassword;
            if ( dbType.compareTo("mysql") == 0 ) {
                DB_URL = "jdbc:mysql://" + sourceHost + "/" + sourceSchema;
                conn = DriverManager.getConnection(DB_URL, sourceUser, sourcePassword);
            } else {
                conn = DriverManager.getConnection(DB_URL);
                type = 2;
            }

            String sqlStr = "";
            ResultSet rs;
            stmt = conn.createStatement();

            // retrieve one record
            if ( type == 1 ) {
                sqlStr = customSql + " LIMIT 0," + sampleChunkSize;
            } else {
                sqlStr = customSql + " OFFSET 1 ROWS FETCH NEXT " + sampleChunkSize + " ROWS ONLY";
            }
 
            logger.info("SQL: " + sqlStr);
            rs = stmt.executeQuery(sqlStr);

            // Get column count                
            final ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // Open csv file and write column headers
            logger.info("Dump extracted data to: " + csvFilePath);
            fw = new FileWriter(csvFilePath);
            for (int i = 1; i <= columnCount; i++) {
                fw.append(rs.getMetaData().getColumnName(i));
                if ( i < columnCount ) fw.append(",");
            }
            fw.append(System.getProperty("line.separator"));
  
            // For each row, loop thru the number of columns and write to the csv file
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (rs.getObject(i) != null) {
                        fw.append(rs.getString(i).replaceAll(",", " "));
                    } else {
                        String data = "null";
                        fw.append(data);
                    }
                    if ( i < columnCount ) fw.append(",");
                }
                fw.append(System.getProperty("line.separator"));
                rowCount += 1;
            }
            fw.flush();
            fw.close();

            String msg1 = String.format("Data extracted: columns=%d  rows=%d\n", columnCount, rowCount);
            logger.info(msg1);

            conn.close();

            if ( runMode == 0 || runMode == 2 ) { 
                // Run script if specified
                String[] command ={pythonPath, scriptPath, tableName, dataDir}; 
                logger.info("Execute registration script: " + scriptPath);
                ProcessBuilder pb = new ProcessBuilder(command);
                try {
                    Process p = pb.start();
                    p.waitFor();
                    p.destroy();
                    logger.info("Register script is executed.");
                } catch (Exception e) {
                    logger.info("Exception during execution of action script." + e.getMessage());
                }
            }
        
            logger.info("Registration process is complete.");
					
        } catch (Exception ex) {
            logger.info("Exception information: " + ex.getMessage());
        }

    }

    // Extract and Upload data to data lake
    private static void upload(JSONObject datalake) {

        FileWriter fw;
        Connection conn = null;
        Statement stmt = null;

        try {
            // Check for script action
            boolean useChunk = true;
            String scriptPath = installDir + "/bin/transform.py";

            // DB connection
            logger.info("Upload: Connecting to database to extract data from " + tableName);
            boolean getNextChunk = true;
            int type = 1;
            String DB_URL = "jdbc:sqlserver://"+sourceHost+":"+sourcePort+";databaseName="+sourceDatabase+";user="+
                sourceUser+";password="+sourcePassword;

            // For prep and upload action, get table column names from data lake
            // only before first extraction of data from SCADA
            String applyDDL = "false";
            if ( collist.length() == 0 ) {
                if ( getDB2TableHeaders(datalake, tableName, clnFilePath) == 1 ) {
                    logger.info("Failed to get column names from Data Lake table " + tableName);
                    applyDDL = "true";
                }
            }

            // create table if needed
            if ( applyDDL.compareTo("true") == 0 && runMode == 0 ) {
                if ( createTable(datalake, tableName, ddlFilePath) == 1 ) {
                    logger.info("Failed to create table in Data Lake: table name " + tableName);
                } else {
                    // get table headers from data lake
                    if ( getDB2TableHeaders(datalake, tableName, clnFilePath) == 1 ) {
                        logger.info("Failed to get column names from Data Lake table " + tableName);
                    }
                }
            }

            // retrieve records
            String limitStr = "";
            long t_stamp = 0;
            long l_t_stamp  = 0;
            int l_t_stamp_convert = -1;
            int nRows = 0;
            long curTmMillis = 0;
            long lastTmMillis = 0;
            String startDate = "";
            while ( getNextChunk ) {
                // Set limit and offset
                int startRow = 0;
                try {
                    String offsetRecordStr = new String (Files.readAllBytes(Paths.get(offFilePath)));
                    JSONObject offsetRecord = new JSONObject(offsetRecordStr);
                    startRow = offsetRecord.getInt("startRow");
                    startDate = offsetRecord.getString("startDate");
                }
                catch (Exception e) {
                    logger.info("Data offset file is not created yet. Start from begining. " + e.getMessage());
                } 

                if ( dbType.compareTo("mysql") == 0 ) {
                    DB_URL = "jdbc:mysql://" + sourceHost + "/" + sourceSchema;
                    conn = DriverManager.getConnection(DB_URL, sourceUser, sourcePassword);
                    limitStr = " LIMIT " + startRow + "," + chunkSize;
                } else {
                    conn = DriverManager.getConnection(DB_URL);
                    type = 2;
                    if ( startRow == 0 ) {
                        logger.info("Invalid LIMIT. Reset to 1.");
                        startRow = 1;
                    }
                    limitStr = " LIMIT OFFSET " + startRow +
                           " ROWS FETCH NEXT " + chunkSize + " ROWS ONLY";
                }

                stmt = conn.createStatement();

                ResultSet rs;
                logger.info("SQL: " + customSql + limitStr);

                try {
                    rs = stmt.executeQuery(customSql + limitStr);
                } catch (Exception qex) {
                    if (qex instanceof SQLException) {
                        int errCode = ((SQLException)qex).getErrorCode();
                        logger.info("Extract: SQLException error code: " + errCode);
                        if (errCode == 1146) {
                            String offsetRec = "{\"startDate\":\"" + startDate + "\",\"startRow\":0,\"lastEndTS\":-1}";
                            fw = new FileWriter(offFilePath);
                            fw.write(offsetRec);
                            fw.flush();
                            fw.close();
                        }
                    }
                    throw qex;
                }

                curTmMillis = System.currentTimeMillis();

                // Get column count and column type of TS column
                final ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
            

                // Open csv file and write column headers    
                fw = new FileWriter(csvFilePath);
                for (int i = 1; i <= columnCount; i++) {
                    String colName = rsmd.getColumnName(i);
                    fw.append(colName);
                    if (l_t_stamp_convert == -1 ) {
                        if ( colName.compareTo(tstampColName) == 0 ) {
                            if (rsmd.getColumnType(i) == java.sql.Types.BIGINT || rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
                                l_t_stamp_convert = 0;
                            } else {
                                l_t_stamp_convert = 1;
                            }
                            logger.info("Convert TS flag for column " + colName + " = " + l_t_stamp_convert); 
                        }
                    }
                    if ( i < columnCount ) fw.append(",");
                }
                fw.append(System.getProperty("line.separator"));

                // For each row, loop thru the number of columns and write to the csv file
                int rowCount = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        if (rs.getObject(i) != null) {
                            fw.append(rs.getString(i).replaceAll(",", " "));
                        } else {
                            String data = "null";
                            fw.append(data);
                        }
                        if ( i < columnCount ) fw.append(",");
                    }
                    fw.append(System.getProperty("line.separator"));
                    rowCount += 1;
                }
                fw.flush();
                fw.close();
               
                // move to last row and get time stamp value
                rs.last();
                nRows = rs.getRow();
                if ( l_t_stamp_convert == 0 ) {
                    l_t_stamp = rs.getLong(tstampColName);
                } else if ( l_t_stamp_convert == 1 ) {
                    l_t_stamp = Timestamp.valueOf(rs.getString(tstampColName)).getTime();
                } 
                String emsg = String.format("RowsExtracted: %d Last_tTtamp: %d", nRows, l_t_stamp);
                logger.info(emsg);

                conn.close();

                // get extracted csv file size
                long csvFileSize = 0;
                long prCsvFileSize = 0;
                try {
                    File exfile = new File(csvFilePath);
                    csvFileSize = exfile.length();
                } catch (Exception e) {}

                String dmsg = String.format("Data extracted: columns=%d  rows=%d\n", columnCount, rowCount); 
                logger.info(dmsg);
   
                // Run script is specified
                String[] command ={pythonPath, scriptPath, tableName, applyDDL}; 
                logger.info("Executing script to transform data.");
                ProcessBuilder pb = new ProcessBuilder(command);
                try {
                    Process p = pb.start();
                    if (p.waitFor(300, TimeUnit.SECONDS) == false) {
                        p.destroyForcibly();
                    } else { 
                        p.destroy();
                    }
                 } catch (Exception e) {
                    logger.info("Exception during execution of action script." + e.getMessage());
                }
                
                // Upload data using batch insert
                int nuploaded = 0;
                if (rowCount > 0 && runMode == 0) {
                    colsProcessed = 0;
                    nuploaded = batchInsert(datalake, tableName, ddlFilePath, prCsvFilePath);
                } else {
                    colsProcessed = 0;
                    nuploaded = rowCount;
                }

                // get processed csv file size
                try {
                    File prfile = new File(prCsvFilePath);
                    prCsvFileSize = prfile.length();
                } catch (Exception e) {}

                // oepn file to write processed data
                if ( nuploaded > 0 || rowCount == 0 ) {
                    fw = new FileWriter(prcFilePath);
                    String procRec = "{ \"processed\":" + nuploaded + ", \"uploaded\":\"Y\" }";
                    fw.write(procRec);
                    fw.flush();
                    fw.close();
                }
    
                if ( nRows < chunkSize || useChunk == false  ) {
                    getNextChunk = false;
                }
                
                // Update data offset file
                // read number of rows processed by script
                int nprocessed = chunkSize;
                String uploaded = "N";
                try {
                    String prcfContent = new String ( Files.readAllBytes( Paths.get(prcFilePath)));
                    JSONObject prcfObject = new JSONObject(prcfContent);
                    nprocessed = prcfObject.getInt("processed");
                    uploaded = prcfObject.getString("uploaded");
                    String msg2 = String.format("Processsed: %d, uploaded: %s", nprocessed, uploaded);
                    logger.info(msg2);
                }
                catch (Exception e) {
                    logger.info("Invalid nprocessed data, set to chunkSize." + e.getMessage());
                    nprocessed = chunkSize;
                }

                // break the loop if no data is uploaded
                if ( uploaded.compareTo("N") == 0 ) { 
                    logger.info("No data is uploaded.");
                    // Update last timestamp is no data is available to upload
                    if (lastTmMillis == 0) {
                        lastTmMillis = curTmMillis;
                    } else {
                        l_t_stamp += (curTmMillis - lastTmMillis);
                        String offsetRec = "{\"startDate\":\"" + startDate + "\", \"startRow\":" + startRow + ", \"lastEndTS\":" + l_t_stamp + " }";
                        fw = new FileWriter(offFilePath);
                        fw.write(offsetRec);
                        fw.flush();
                        fw.close();
                        lastTmMillis = curTmMillis;
                    }
                    break;
                }

                // Write upload stats for accounting and offsetRec for next update
                // Do not close stats file
                String statsMsg = "";
                String offsetRec = "";
                startRow = startRow + nprocessed;
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                String curTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(ts);
                statsMsg = String.format(
                    "%s, %d, %d, %d, %d, %d, %d, %s, %d\n",
                    curTime, csvFileSize, columnCount, rowCount, prCsvFileSize, colsProcessed, nprocessed, uploaded, l_t_stamp);
                offsetRec = "{\"startDate\":\"" + startDate + "\", \"startRow\":" + startRow + ", \"lastEndTS\":" + l_t_stamp + " }";
                fwStats.write(statsMsg);
                fwStats.flush();
                fw = new FileWriter(offFilePath);
                fw.write(offsetRec);
                fw.flush();
                fw.close();
                lastTmMillis = curTmMillis;

                // remove temprary process file
                File prcfile = new File(prcFilePath);
                prcfile.delete();

                try {
                    Thread.sleep(5000);
                } catch (Exception e) {}

                // For testMode stop the loop
                if ( runMode == 3 ) {
                    getNextChunk = false;
                    logger.info("Test mode is enabled. Stop chuck processing loop");
                }
            }
        
            logger.info("Data processing cycle is complete.");
					
        } catch (Exception ex) {
            logger.info("Exception information: " + ex.getMessage());
        }
    }

    // Get table headers from WIoTP Data Lake (DB2)
    private static int getDB2TableHeaders(JSONObject datalake, String tableName, String clnFilePath) {
        Connection con;
        Statement stmt;
        ResultSet rs;
        int retval = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            
            JSONObject obj = new JSONObject();

            // Get data lake config items
            String host = (String)datalake.get("host");
            String port = (String)datalake.get("port");
            String user = (String)datalake.get("user");
            String password = (String)datalake.get("password");
            String urlSSL = "jdbc:db2://" + host + ":" + port + "/BLUDB:sslConnection=true;";
            String colTitle;

            logger.info("Connecting to Data Lake to get table column names.");
            con = DriverManager.getConnection(urlSSL, user, password);
            con.setAutoCommit(false);
            stmt = con.createStatement();

            String tabname = "IOT_" + tableName.toUpperCase();
            String sql = "select * from SYSIBM.SYSCOLUMNS where tbname='" + tabname + "' order by colno";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                colTitle = rs.getString(1);
                collist.put(colTitle);
            }

            obj.put("ColumnTitle", collist);
            logger.info(obj.toString());

            FileOutputStream outputStream = new FileOutputStream(clnFilePath);
            outputStream.write(obj.toString().getBytes());
            outputStream.close();

            rs.close();

            if ( collist.length() > 0 ) {
                String delSql = "delete from (select * from " + tabname + " fetch first 1 rows only)";
                rs = stmt.executeQuery(sql);
                rs.close();
            } else {
                retval = 1;
            }

            stmt.close();
            con.commit();
            con.close();
        }
        catch (Exception ex) {
            logger.info("Exception information:");
            if (ex != null) {
                logger.info("Error msg: " + ex.getMessage());
            }
            retval = 1;
        }

        return retval;
    }

    // Create table in WIoTP Data Lake (DB2)
    private static int createTable(JSONObject datalake, String tableName, String ddlFilePath) {
        Connection con;
        Statement stmt;
        ResultSet rs;
        int retval = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            
            JSONObject obj = new JSONObject();

            // Get data lake config items
            String host = (String)datalake.get("host");
            String port = (String)datalake.get("port");
            String user = (String)datalake.get("user");
            String password = (String)datalake.get("password");
            String urlSSL = "jdbc:db2://" + host + ":" + port + "/BLUDB:sslConnection=true;";
            String colTitle;

            logger.info("Create table " + tableName);
            con = DriverManager.getConnection(urlSSL, user, password);
            con.setAutoCommit(false);
            stmt = con.createStatement();
            String sql = new String ( Files.readAllBytes( Paths.get(ddlFilePath) ) );
            logger.info("SQL: " + sql);
            stmt.executeUpdate(sql);
            stmt.close();
            con.commit();
            con.close();
        }
        catch (Exception ex) {
            logger.info("Exception information:");
            if (ex != null) {
                logger.info("Error msg: " + ex.getMessage());
            }
            retval = 1;
        }

        return retval;
    }

    // Batch insert in WIoTP Data Lake (DB2) from CSV file
    private static int batchInsert(JSONObject datalake, String tableName, String ddlFilePath, String prCsvFilePath) {
        Connection con;
        Statement stmt;
        ResultSet rs;
        int retval = 0;
        String line = ""; 
        int rowsProcessed = 0;
        int batchCount = 0;
        
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            
            // Get data lake config items
            String host = (String)datalake.get("host");
            String port = (String)datalake.get("port");
            String user = (String)datalake.get("user");
            String password = (String)datalake.get("password");
            String urlSSL = "jdbc:db2://" + host + ":" + port + "/BLUDB:sslConnection=true;";
         
            // Create SQL for inserting records
            if ( insertSQL.length() == 0 ) {
                logger.info("Read DDL file to get table column name and data types.");
                String ddlStr = "";
                String colnames = "";
                String vals = "";

                ddlStr = new String ( Files.readAllBytes( Paths.get(ddlFilePath) ) );
                logger.info("Use DDL data: " + ddlStr);

                String [] ddlParts = ddlStr.split("\\(", 2);
                logger.info("ddlParts[0]: " + ddlParts[0]);
                logger.info("ddlParts[1]: " + ddlParts[1]);

                String [] columns = ddlParts[1].split(",+");
                logger.info("Number of table columns to be added: " + columns.length);
                String [] ddlcname = new String[MAX_TABLE_COLUMNS];
                String [] ddlctype = new String[MAX_TABLE_COLUMNS];
                colsProcessed = columns.length;

                for (int i=0; i<columns.length; i++) {
                    String [] colParts = columns[i].trim().split(" ");
                    ddlcname[i] = colParts[0];
                    ddlctype[i] = "char";
                    if ( colParts[1].toUpperCase().contains("DOUBLE")) {
                        ddlctype[i] = "double";
                    } else if ( colParts[1].toUpperCase().contains("NUMBER")) {
                        ddlctype[i] = "double";
                    } else if ( colParts[1].toUpperCase().contains("INTEGER")) {
                        ddlctype[i] = "double";
                    } else if ( colParts[1].toUpperCase().contains("TIMESTAMP")) {
                        ddlctype[i] = "timestamp";
                    }
                }

                for (int j=0; j<collist.length(); j++) {
                    String colName = collist.get(j).toString();
                    if ( j > 0 ) {
                        colnames = colnames + ", ";
                        vals = vals + ", ";
                    }

                    int found = 0;
                    for (int i=0; i<columns.length; i++) {
                        if ( colName.compareTo(ddlcname[i]) == 0 ) {
                            ctype[j] = ddlctype[i];
                            logger.fine("Datalake: Column=" + colName + " Type=" + ctype[j]);
                            colnames = colnames + colName;
                            vals = vals + "?"; 
                            found = 1;
                            break;
                        }
                    }
         
                    if ( found == 0 ) {
                        logger.info("ERROR: Could not find type for " + colName);
                    }
                }
    
                String tabname = "IOT_" + tableName.toUpperCase();
                insertSQL = "INSERT INTO " + tabname + " (" + colnames + ") VALUES (" + vals + ")";
                logger.info("SQL Stmt: " + insertSQL);
                logger.info("Use CSV file to batch update: " + prCsvFilePath);
            }

            logger.info("Connect to data lake");
            con = DriverManager.getConnection(urlSSL, user, password);
            con.setAutoCommit(false);

            PreparedStatement ps = con.prepareStatement(insertSQL);
            BufferedReader bReader = new BufferedReader(new FileReader(prCsvFilePath));
            while ((line = bReader.readLine()) != null) {
                try {
                    if (line != null) {
                        // Skip header line
                        if ( rowsProcessed == 0 ) {
                            rowsProcessed += 1;
                            continue;
                        }

                        String[] array = line.split(",");

                        if ( rowsProcessed < 2 ) {
                            logger.fine("Line: " + array.length + " - " + line);
                        }
                        for (int i=0; i<array.length; i++) {
                            int index = i + 1;
                            if (ctype[i] == "char") {
                                ps.setString(index, array[i]);
                            } else if (ctype[i] == "double") {
                                double d = Double.parseDouble(array[i]);
                                ps.setDouble(index,d);
                            } else if (ctype[i] == "timestamp") {
                                Timestamp ts = Timestamp.valueOf(array[i]);
                                ps.setTimestamp(index,ts);
                            }
                        }

                        ps.addBatch();
                        batchCount += 1;
                        rowsProcessed += 1; 
                    }
                    if ( batchCount >= batchInsertSize ) {
                        String msg = String.format("Batch update table: count:%d", batchCount);
                        logger.info(msg);
                        ps.executeBatch();
                        con.commit();
                        ps.clearBatch();
                        batchCount = 0;
                    }
                } catch (Exception ex) {
                    if ( line != null ) {
                        logger.info("Line: " + line);
                    }
                    ex.printStackTrace();
                    break;
                }
                finally {
                    if (bReader == null) {
                        bReader.close();
                    }
                }
            }
            if ( batchCount > 0 ) {
                String msg = String.format("Batch update table. count:%d", batchCount);
                logger.info(msg);
                ps.executeBatch();
                con.commit();
                ps.clearBatch();
            }
            con.commit();
            con.close();
            retval = rowsProcessed - 1;
            String msg = String.format("Total rows processed: %d", retval);
            logger.info(msg);
        }
        catch (Exception e) {
            logger.info("rowsProcessed: " + rowsProcessed + " batchCount: " + batchCount);
            logger.info("Line: " + line);
            e.printStackTrace();
        }

        return retval;
    }


    // Extract data using defined sql and dump it in csv file
    private static void extractOnly(String sqlFileName) {

        FileWriter fw;
        Connection conn = null;
        Statement stmt = null;

        try {
            // Read file content from sql file
            String custSqlFilePath = dataDir + "/volume/config/" + sqlFileName;
            String custCsvFilePath = dataDir + "/volume/data/csv/" + sqlFileName.replace(".sql", ".csv");
            String sqlStatement = new String(Files.readAllBytes(Paths.get(custSqlFilePath)));

            // DB connection
            logger.info("Connecting to database to extract data from " + tableName);
            int type = 1;
            String DB_URL = "jdbc:sqlserver://"+sourceHost+":"+sourcePort+";databaseName="+sourceDatabase+";user="+
                sourceUser+";password="+sourcePassword;
            if ( dbType.compareTo("mysql") == 0 ) {
                DB_URL = "jdbc:mysql://" + sourceHost + "/" + sourceSchema;
                conn = DriverManager.getConnection(DB_URL, sourceUser, sourcePassword);
            } else {
                conn = DriverManager.getConnection(DB_URL);
                type = 2;
            }

            String sqlStr = "";
            ResultSet rs;
            stmt = conn.createStatement();

            // retrieve one record
            if ( type == 1 ) {
                sqlStr = sqlStatement + " LIMIT 0,5000";
            } else {
                sqlStr = sqlStatement + " OFFSET 1 ROWS FETCH NEXT 5000 ROWS ONLY";
            }
 
            logger.info("SQL: " + sqlStr);
            rs = stmt.executeQuery(sqlStr);

            // Get column count                
            final ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // Open csv file and write column headers
            logger.info("Dump extracted data to: " + custCsvFilePath);
            fw = new FileWriter(custCsvFilePath);
            for (int i = 1; i <= columnCount; i++) {
                fw.append(rs.getMetaData().getColumnName(i));
                if ( i < columnCount ) fw.append(",");
            }
            fw.append(System.getProperty("line.separator"));
  
            // For each row, loop thru the number of columns and write to the csv file
            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (rs.getObject(i) != null) {
                        fw.append(rs.getString(i).replaceAll(",", " "));
                    } else {
                        String data = "null";
                        fw.append(data);
                    }
                    if ( i < columnCount ) fw.append(",");
                }
                fw.append(System.getProperty("line.separator"));
                rowCount += 1;
            }
            fw.flush();
            fw.close();

            String msg1 = String.format("Data extracted: columns=%d  rows=%d\n", columnCount, rowCount);
            logger.info(msg1);

            conn.close();
        } catch (Exception ex) {
            logger.info("Exception information: " + ex.getMessage());
        }
    }
}
