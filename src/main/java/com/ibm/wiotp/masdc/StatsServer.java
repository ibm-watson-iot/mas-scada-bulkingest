/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.*;
import java.io.OutputStream;
import java.io.File;
import java.net.URI;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
 
public class StatsServer {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");
 
    private static HttpServer httpServer;
    private static Config config;
    private static OffsetRecord offsetRecord;
    private static int port = 0;
    private static int running = 0;
    private static String enableWebUIFile;

    public StatsServer(Config config, OffsetRecord offsetRecord) {
        this.config = config;
        this.offsetRecord = offsetRecord;
        port = config.getHttpPort();
        enableWebUIFile = config.getEnableWebUIFile();
        logger.info(String.format("HTTP Port: %d", port));
        if (port != 0) {
            try {
                String context = "/masdcStats";
                httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
                httpServer.createContext(context, new HttpHandler() {
                    public void handle(HttpExchange he) throws IOException {
                        URI uri = he.getRequestURI();
                        String response = createResponse(uri);
                        he.sendResponseHeaders(200, response.getBytes().length);
                        OutputStream os = he.getResponseBody();
                        os.write(response.getBytes());
                        os.close();
                    }
                });
                httpServer.setExecutor(null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        startStatsThread();
    }

    private String createResponse(URI uri) {
        StringBuilder sb =  new StringBuilder("<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML 2.0//EN'> \r\n");
        sb.append("<html> <head> <title>MAS Ignition Data Connector</title> </head> <body> \r\n");
        sb.append("<h3 style=\"color:white;background-color:black;\"> &nbsp; IBM MAS Ignition Connector</h3> \r\n");
        sb.append("<h4>Current Status</h4> \r\n");
        sb.append(String.format("<pre>Client Site: %s \r\n", config.getClientSite()));
        sb.append(String.format("Entity Type: %s \r\n", config.getEntityType()));
        sb.append(String.format("Total Registered Tags: %d \r\n", offsetRecord.getEntityCount()));
        sb.append(String.format("Total Registered Types: %d \r\n", offsetRecord.getEntityTypeCount()));
        sb.append(String.format("\r\n"));
        sb.append(String.format("Last processing cycle stats:\r\n"));
        sb.append(String.format("Extracted Records: %d \r\n", offsetRecord.getProcessedCount()));
        sb.append(String.format("Uploaded Records: %d \r\n", offsetRecord.getUploadedCount()));
        sb.append(String.format("Process Rate per second: %d \r\n", offsetRecord.getRate()));
        sb.append(String.format("Last data extract start time (in seconds): %d \r\n", offsetRecord.getStartTimeSecs()));
        sb.append(String.format("Last data extract end   time (in seconds): %d \r\n", offsetRecord.getEndTimeSecs()));
        sb.append("</pre> \r\n");
        sb.append("</body> </html> \r\n");
        return sb.toString();
    }

    // Thread to create devices
     private static void startStatsThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while(true) {
                    logger.info("Check/Start local Stats server");
                    if (config.getUpdateFlag() == 1) break;

                    // check if http server is enabled
                    File f = new File(enableWebUIFile);
                    if (f.exists()) {
                        if (port != 0 && running == 0) {
                            logger.info("Start Web Server");
                            httpServer.start();
                            running = 1;
                        }
                    } else {
                        if (running == 1) {
                            logger.info("Stop Web Server");
                            httpServer.stop(1);
                            running = 0;
                        }
                    }

                    try {
                        Thread.sleep(30000);
                    } catch (Exception e) {}
                }
            }
        };
        logger.info("Starting stats thread ...");
        new Thread(thread).start();
        logger.info("Stats thread is started");
        return;
    }
 
}

