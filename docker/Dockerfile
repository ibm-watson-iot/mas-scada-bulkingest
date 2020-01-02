#
#################################################################
#
#  Licensed Materials - Property of IBM
#  (C) Copyright IBM Corp. 2019 All Rights Reserved.
#
#  US Government Users Restricted Rights - Use, duplication or
#  disclosure restricted by GSA ADP Schedule Contract with
#  IBM Corp.
#
#################################################################
#
# Dockerfile to build mas-dataconnector container image
#


FROM ubuntu

COPY ./bin/requirements.txt /tmp/requirements.txt
RUN apt-get update && \
    apt-get -y install vim python3-pip unzip inotify-tools net-tools openssl ca-certificates jq apache2 e2fsprogs libzstd1 libidn2-0 default-jre && \
    apt-get clean && \
    pip3 install --upgrade -r /tmp/requirements.txt && \
    mkdir -p /ibm/masdc && \
    mkdir -p /volume/logs

COPY bin /ibm/masdc/bin
COPY lib /ibm/masdc/lib
COPY ./docker/entry.sh /ibm/masdc/bin/entry.sh
RUN chmod +x /ibm/masdc/bin/entry.sh

WORKDIR /volume/logs

ENTRYPOINT [ "/ibm/masdc/bin/entry.sh" ]

