#!/bin/bash

export NABU_LOG_PROCESSOR_BACKEND_ENDPOINT=http://34.171.111.18:5200/v3/buildspan
export NABU_TRACING_LOG_PATH=~/.ipfs/
export LOG_EXPORTER_STATE_PATH=~/.ipfs/state/

java -cp target/nabu-log-exporter-1.0-SNAPSHOT.jar com.github.millerm.NabuLogExporter
