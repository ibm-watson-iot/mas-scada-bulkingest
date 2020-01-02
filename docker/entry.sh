#!/bin/bash
#
# IBM Maximo Application Suite - SCADA Bulk Data Ingest Connector
#
# *****************************************************************************
# Copyright (c) 2019 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************
#
#
# mas-dataconnector container entry point script
#
# Entry point script does the following:
#
# - Runs the following steps when a new configuration file is availavle in /volume/config directory:
#   - Prepare input CSV file in a format required by WIoTP Data Lake
#   - Create DDL file for creating tables in WIoTP Data Lake (IBM DB2 Cloud)
#   - Run Lift CLI to ingest data from CSV file in IBM DB2 Cloud
#

# MAS Data Connector home
MASDCHOME=$HOME/ibm/masdc

export PATH=$PATH:${MASDCHOME}/bin

mkdir -p ${MASDCHOME}/volume/logs
LOGFILE="${MASDCHOME}/volume/logs/masdc.log"
touch $LOGFILE

echo "" >> $LOGFILE
echo "" >> $LOGFILE
echo "----------------------------------" >> $LOGFILE
echo " MAS Data Connector " >> $LOGFILE
echo "----------------------------------" >> $LOGFILE
echo "Start Time: `date`" >> $LOGFILE
echo "" >> $LOGFILE

#
# Run install script is not done yet
#
if [ ! -f ${MASDCHOME}/.installed ]; then
    chmod +x ${MASDCHOME}/bin/install.sh
    ${MASDCHOME}/bin/install.sh >> $LOGFILE 2>&1 3>&1
    touch ${MASDCHOME}/.installed
fi

echo "" >> $LOGFILE
touch ${MASDCHOME}/volume/config/.inotify_disable

#
# Outer loop is to handle inotifywait crash or abnormal exit for any reason,
# and keep the container running.
# 
LOOP=1
while [ $LOOP -gt 0 ];
do

    # Use inotify if configured
    if [ -f ${MASDCHOME}/volume/config/.inotify_disable ]
    then
	echo "Data connector is configured for manual configuration and processing." >> $LOGFILE
        sleep 60
        continue
    fi
    
    # wait for a new csv file in /volume/data/csv directory
    inotifywait -m ${MASDCHOME}/volume/config/ -e close_write -e create |
        while read directory event file; do
            filename="${file##*/}"
	    extension="${filename##*.}"
            # echo "inotify file event. file: $filename" >> $LOGFILE
            if [ "${extension}" == "json" ]
            then
                echo "Processing new or updated configuration file: $filename" >> $LOGFILE
                dtype=$(echo $filename | cut -d"." -f1)
                mkdir -p ${MASDCHOME}/volume/data/$dtype/schemas
                mkdir -p ${MASDCHOME}/volume/data/$dtype/data
                ps -ef | grep "connector.sh" | grep $dtype > /dev/null 2>&1
                if [ $? -eq 0 ]; then
                    echo "Process for entity type $dtype is running." >> $LOGFILE
                else
                    echo "Start process for entity type $dtype." >> $LOGFILE
                    ${MASDCHOME}/bin/connector.sh $dtype
                fi
		sleep 5
	    fi
        done

    echo "inotifywait exited with exit code: $?" >> $LOGFILE
    echo "Restart inotifywait" >> $LOGFILE
    sleep 10

done

echo "----------------------------------" >> $LOGFILE
echo "-------- Stop Entry script -------" >> $LOGFILE
echo "----------------------------------" >> $LOGFILE
echo "" >> $LOGFILE

