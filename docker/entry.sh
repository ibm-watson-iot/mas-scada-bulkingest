#!/bin/bash
#
##############################################################################################
#
#  Licensed Materials - Property of IBM
#  (C) Copyright IBM Corp. 2019 All Rights Reserved.
#
#  US Government Users Restricted Rights - Use, duplication or
#  disclosure restricted by GSA ADP Schedule Contract with
#  IBM Corp.
#
##############################################################################################
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

export PATH=$PATH:/opt/ibm/masdc/bin

mkdir -p /volume/logs
LOGFILE="/volume/logs/masdc.log"
touch $LOGFILE

echo "" >> $LOGFILE
echo "" >> $LOGFILE
echo "----------------------------------" >> $LOGFILE
echo " MAS Data Connector " >> $LOGFILE
echo "----------------------------------" >> $LOGFILE
echo "Start Time: `date`" >> $LOGFILE
echo "" >> $LOGFILE

#
# Start doc server
#
/usr/sbin/apachectl start
if [ ! -f /opt/ibm/masdc/.http_ssl ]
then
    mkdir -p /volume/config/certs
    cp /opt/ibm/masdc/certs/apache.crt /volume/config/certs/.
    cp /opt/ibm/masdc/certs/apache.key /volume/config/certs/.
    a2enmod ssl
    a2ensite default-ssl.conf
    /usr/sbin/apachectl restart
    touch /opt/ibm/masdc/.http_ssl
fi

echo "" >> $LOGFILE
touch /volume/config/.inotify_disable

#
# Outer loop is to handle inotifywait crash or abnormal exit for any reason,
# and keep the container running.
# 
LOOP=1
while [ $LOOP -gt 0 ];
do

    # Use inotify if configured
    if [ -f /volume/config/.inotify_disable ]
    then
	echo "Data connector is configured for manual configuration and processing." >> $LOGFILE
        sleep 60
        continue
    fi
    
    # wait for a new csv file in /volume/data/csv directory
    inotifywait -m /volume/config/ -e close_write -e create |
        while read directory event file; do
            filename="${file##*/}"
	    extension="${filename##*.}"
            # echo "inotify file event. file: $filename" >> $LOGFILE
            if [ "${extension}" == "json" ]
            then
                echo "Processing new configuration file: $filename" >> $LOGFILE
                dtype=$(echo $filename | cut -d"." -f1)
                mkdir -p /volume/data/$dtype/schemas
                mkdir -p /volume/data/$dtype/data
                echo "Process device type: $dtype" >> $LOGFILE
                /opt/ibm/masdc/bin/connector.sh $dtype &
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

