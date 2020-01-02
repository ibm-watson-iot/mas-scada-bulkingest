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
# Makefile to build wiotp-bulkdata-ingest container using IBM lift CLI
# for uploading bulk data exported from SCADA systems in CSV format to
# IBM DB2 Cloud.
#


export DOCKER_HUB_ID =
export DOCKER_HUB_PASSWORD =
export DOCKER_IMAGE_NAME = mas-dataingest
export DOCKER_IMAGE_VERSION = 1.0

ifdef DOCKER_HUB_ID
export DOCKER_IMAGE_PATH = $(DOCKER_HUB_ID)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
else
export DOCKER_IMAGE_PATH = $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
endif

default: all

all: dockerremove dockerimage dockerrun

buildjar:
	@echo Build utilities
	mvn clean
	mvn package
	cp target/*.jar lib/.

dockerimage:
	@echo Build docker image $$DOCKER_IMAGE_NAME
	docker build -f ./docker/Dockerfile -t $(DOCKER_IMAGE_PATH) .

dockerrun:
	@echo Run $$DOCKER_IMAGE_NAME container
	@mkdir -p `pwd`/volume
	docker run -dit --name $(DOCKER_IMAGE_NAME) --volume `pwd`/volume:/root/ibm/masdc/volume $(DOCKER_IMAGE_PATH)

dockercheck:
	@echo Check $$DOCKER_IMAGE_NAME container
	docker logs $(DOCKER_IMAGE_NAME)

dockerremove:
	@echo Stop and remove $$DOCKER_IMAGE_NAME container
	-docker rm -f $(DOCKER_IMAGE_NAME) 2> /dev/null || :
	-docker rmi -f $(DOCKER_IMAGE_PATH) 2> /dev/null || :

dockerstop:
	@echo Stop $$DOCKER_IMAGE_NAME container
	-docker stop $(DOCKER_IMAGE_NAME) 2> /dev/null || :

dockerstart:
	@echo Start $$DOCKER_IMAGE_NAME container
	-docker start $(DOCKER_IMAGE_NAME) 2> /dev/null || :

dockerpush:
	@echo Push docker image in IBM Container registry
	docker tag ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_VERSION} ${DOCKER_IMAGE_PATH}
	docker login -u ${DOCKER_HUB_ID} -p ${DOCKER_HUB_PASSWORD}; docker push ${DOCKER_IMAGE_PATH}

docs: mkdocs/mkdocs.yml mkdocs/docs/*.md
	@echo "Build documentation"
	@cd mkdocs; mkdocs build -d ../docs

install:
	@echo Install MAS Data Connector on localhost
	-cd bin; chmod +x install.sh; ./install.sh localhost

