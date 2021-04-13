/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

import java.io.*;
import java.net.*;
import java.util.logging.*;
 
public class CLIServer {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private static Config config;
    private static OffsetRecord offsetRecord;
    private static int serverPort;

    public CLIServer(Config config, OffsetRecord offsetRecord) {
        this.config = config;
        this.offsetRecord = offsetRecord;
        serverPort = config.getCLIPort();
    }

    public void start() {
        startCLIServerThread();
    }

    private static String getStats() {
        StringBuilder sb =  new StringBuilder("\r\nIBM MAS Ignition Connector Stats\r\n");
        sb.append(String.format("\r\n"));
        sb.append(String.format("Client Site: %s \r\n", config.getClientSite()));
        sb.append(String.format("Entity Type: %s \r\n", config.getEntityType()));
        sb.append(String.format("Total Registered Tags: %d \r\n", offsetRecord.getEntityCount()));
        sb.append(String.format("\r\n"));
        sb.append(String.format("Last processing cycle stats:\r\n"));
        sb.append(String.format("Extracted Records: %d \r\n", offsetRecord.getProcessedCount()));
        sb.append(String.format("Uploaded Records: %d \r\n", offsetRecord.getUploadedCount()));
        sb.append(String.format("Process Rate per second: %d \r\n", offsetRecord.getRate()));
        sb.append(String.format("Last data extract start time (in seconds): %d \r\n", offsetRecord.getStartTimeSecs()));
        sb.append(String.format("Last data extract end   time (in seconds): %d \r\n", offsetRecord.getEndTimeSecs()));
        return sb.toString();
    }

    private static String setDebug() {
        logger.setLevel(Level.FINE);
        logger.fine("Log level is set to FINE");
        return "Debug level is set.";
    }

    private static String unsetDebug() {
        logger.setLevel(Level.INFO);
        logger.info("Log level is set to INFO");
        return "Debug level is unset.";
    }

    // Thread to create devices
     private static void startCLIServerThread() {
        Runnable thread = new Runnable() {
            public void run() {
                while (true) {
                    try {
                        ServerSocket serverSocket = new ServerSocket(serverPort, 0, InetAddress.getByName("127.0.0.1"));
                        while(true) {
                            logger.info("Waiting for client on port: " + serverSocket.getLocalPort()); 
                            Socket server = serverSocket.accept();
                            logger.info("CLI request received from: " + server.getRemoteSocketAddress()); 
                            PrintWriter response = new PrintWriter(server.getOutputStream(),true);
                            BufferedReader request = new BufferedReader(new InputStreamReader(server.getInputStream()));
                            String command = request.readLine();
                            String responseStr = "Invalid request";
                            if (command.equals("getStats")) {
                                responseStr = getStats(); 
                            } else if (command.equals("setDebug")) {
                                responseStr = setDebug(); 
                            } else if (command.equals("unsetDebug")) {
                                responseStr = unsetDebug(); 
                            }
                            response.println(responseStr);
                            response.flush();
                            response.close();
                            request.close();
                        }
                    }
                    catch(Exception e) {
                        logger.info("Exception caught: " + e.getMessage());
                        logger.info("Restart CLI server.");
                    }
                }
            }
        };
        logger.info("Starting CLI server thread ...");
        new Thread(thread).start();
        logger.info("CLI Server thread is started");
        return;
    }

}

