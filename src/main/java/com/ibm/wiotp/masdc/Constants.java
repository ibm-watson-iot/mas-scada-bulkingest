/*
 *  Copyright (c) 2020-2021 IBM Corporation and other Contributors.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.wiotp.masdc;

public class Constants {

    private Constants() {
    }

    public static final int PRODUCTION = 0;
    public static final int DEBUG = 1;
    public static final int TEST = 2;

    public static final int CONNECTOR_DEVICE = 1;
    public static final int CONNECTOR_ALARM = 2;

    public static final int AUTH_BASIC = 1;
    public static final int AUTH_HEADER = 2;

    public static final int DB_SOURCE_TYPE_MYSQL = 1;
    public static final int DB_SOURCE_TYPE_MSSQL = 2;
    public static final int DB_DEST_TYPE_DB2 = 1;
    public static final int DB_DEST_TYPE_POSTGRE = 2;

    public static final int CONNECTOR_STATS_TAGS = 6;

    public static final int EXTRACT_STATUS_INIT = 0;
    public static final int EXTRACT_STATUS_TABLE_WITH_DATA = 1;
    public static final int EXTRACT_STATUS_TABLE_NO_DATA = 2;
    public static final int EXTRACT_STATUS_NO_TABLE = 3;

}


