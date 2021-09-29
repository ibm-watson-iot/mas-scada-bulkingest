#
# *****************************************************************************
# Copyright (c) 2021 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
#
# *****************************************************************************
#
# Makefile to build mas-scada-bulkingest project
#

default: all

all: buildjar

buildjar:
	@echo Build utilities
	mkdir -p testdata/volume/data
	cp -r config testdata/volume/.
	mvn clean
	mvn package
	cp target/*.jar lib/.

docs: mkdocs/mkdocs.yml mkdocs/docs/*.md
	@echo "Build documentation"
	@cd mkdocs; mkdocs build -d ../docs

install:
	@echo Install MAS Data Connector on localhost
	-cd bin; chmod +x install.sh; ./install.sh localhost

