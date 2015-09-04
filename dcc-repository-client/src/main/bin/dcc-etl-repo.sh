#!/bin/bash
#
# Copyright 2015(c) The Ontario Institute for Cancer Research. All rights reserved.
#
# Description:
#   Runs dcc-etl-repo
#
# Usage:
#   ./dcc-repository-client.sh [--sources <sources>]
#
# Example:
#   ./dcc-repository-client.sh --sources cghub,pcawg,tcga

base_dir=$(dirname $0)/..
java_opts="-Xmx4g"

java \
	${java_opts} \
	-Dlogback.configurationFile=${base_dir}/conf/logback.xml \
	-jar ${base_dir}/lib/dcc-repository-client.jar --config ${base_dir}/conf/config.yaml "$@"