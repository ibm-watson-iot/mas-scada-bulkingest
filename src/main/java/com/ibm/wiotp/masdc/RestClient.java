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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.logging.*;

public class RestClient {

    private static final Logger logger = Logger.getLogger("mas-ignition-connector");

    private String baseUri = "";
    private int authType = 0;
    private String key = "";
    private String token = "";
    private HttpClient client = null;
    private HttpRequest request = null;
    private HttpResponse<Path> responseFile = null;
    private HttpResponse<Void> responseNone = null;
    private String outFile = "";

    public RestClient(String baseUri, int authType, String key, String token, String outFile) {
        this.baseUri = baseUri;
        this.authType = authType;
        this.key = key;
        this.token = token;
        this.outFile = outFile;
        this.client = HttpClient.newHttpClient();
    }

    public void post(String method, String body) throws IOException, InterruptedException {
        String postEndpoint = this.baseUri + method;
        logger.info("POST Endpoint: " + postEndpoint);
        if (authType == Constants.AUTH_BASIC) {
            String encodedAuth = Base64.getEncoder()
                .encodeToString((this.key + ":" + this.token).getBytes(StandardCharsets.UTF_8));

            request = HttpRequest.newBuilder()
                .uri(URI.create(postEndpoint))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        } else {
            request = HttpRequest.newBuilder()
                .uri(URI.create(postEndpoint))
                .header("Content-Type", "application/json")
                .header("x-api-key", key)
                .header("x-api-token", token)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        }
        if (outFile != null && !outFile.isEmpty()) {
            responseFile = client.send(request, HttpResponse.BodyHandlers.ofFile(Paths.get(outFile)));
        } else {
            responseNone = client.send(request, HttpResponse.BodyHandlers.discarding());
        }
    }

    public void get(String method) throws IOException, InterruptedException {
        String getEndpoint = this.baseUri + method;
        logger.info("GET Endpoint: " + getEndpoint);
        if (authType == 1) {
            request = HttpRequest.newBuilder()
                .uri(URI.create(getEndpoint))
                .header("Content-Type", "application/json")
                .build();
        } else {
            request = HttpRequest.newBuilder()
                .uri(URI.create(getEndpoint))
                .header("Content-Type", "application/json")
                .header("x-api-key", key)
                .header("x-api-token", token)
                .build();
        }
        if (outFile != null && !outFile.isEmpty()) {
            responseFile = client.send(request, HttpResponse.BodyHandlers.ofFile(Paths.get(outFile)));
        } else {
            responseNone = client.send(request, HttpResponse.BodyHandlers.discarding());
        }
    }

    public int getResponseCode() {
        if (outFile != null && !outFile.isEmpty()) {
            return(this.responseFile.statusCode());
        }
        return(this.responseNone.statusCode());
    }

}

