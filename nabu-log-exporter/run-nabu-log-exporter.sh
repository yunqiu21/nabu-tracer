#!/bin/bash

export NABU_TRACING_LOG_PATH=/home/iamsushantgupta/.ipfs/
export LOG_EXPORTER_STATE_PATH=/home/iamsushantgupta/.ipfs/state/
sudo -E java -cp target/nabu-log-exporter-1.0-SNAPSHOT.jar com.github.millerm.NabuLogExporter
