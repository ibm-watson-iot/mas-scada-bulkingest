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
# Dockerfile to build mas-dataconnector container image
#


FROM ubuntu
ENV HOME /root
COPY bin/requirements.txt /tmp/requirements.txt
RUN apt-get update && \
    apt-get -y install curl vim python3-pip inotify-tools net-tools openssl ca-certificates jq && \
    apt-get clean && \
    pip3 install --upgrade -r /tmp/requirements.txt && \
    mkdir -p /ibm/masdc && \
    mkdir -p /ibm/masdc/volume/logs
WORKDIR $HOME/ibm/masdc/volume/logs
COPY bin $HOME/ibm/masdc/bin
COPY lib $HOME/ibm/masdc/lib
COPY ./docker/entry.sh /bin/entry.sh
RUN chmod +x /bin/entry.sh

ENTRYPOINT [ "/bin/entry.sh" ]

