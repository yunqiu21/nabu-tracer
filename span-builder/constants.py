from enum import Enum

# Jaeger Config
JAEGER_ENDPOINT = "http://34.67.248.229:4318/v1/traces"
SERVICE_NAME = "nabu"

# Keys to parse JSON from Daemon processor
RAW_LOG_TRACE_ID_KEY = "traceId"
RAW_LOG_SPAN_ID_KEY = "spanId"  # TODO: Ideally, this comes from daemon process
RAW_LOG_NODE_ID_KEY = "nodeId"
RAW_LOG_PEER_NODE_ID_KEY = "peerNodeId"
RAW_LOG_THREAD_ID_KEY = "threadId"
RAW_LOG_TIME_STAMP_KEY = "timestamp"
RAW_LOG_EVENT_TYPE_KEY = "eventType"

# Function START and STOP
Stage = Enum("Stage", ["START", "END"])

# Keys to construct Jaeger trace
JAEGER_TRACE_ID_KEY = "traceId"
JAEGER_SPAN_ID_KEY = "spanId"
JAEGER_PARENT_SPAN_ID_KEY = "parentSpanId"
JAEGER_START_TIME_NANO_KEY = "startTimeUnixNano"
JAEGER_END_TIME_NANO_KEY = "endTimeUnixNano"
JAEGER_SPAN_OPERATION_NAME_KEY = "name"
JAEGER_SPAN_KIND_KEY = "kind"

# Starter span to build Jaeger trace
STARTER_SPAN = {
    "resourceSpans": [
        {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": SERVICE_NAME}}
                ]
            },
            "scopeSpans": [{"spans": []}],
        }
    ]
}

GET_PROVIDERS_CLIENT = "GET_PROVIDERS_CLIENT"
GET_PROVIDERS_SERVER = "GET_PROCIDERS_SERVER" 
BITSWAP_CLIENT = "BITSWAP_CLIENT"
BITSWAP_SERVER = "BITSWAP_SERVER"
READ_FROM_FILE_STORE = "READ_FROM_FILE_STORE"

NABU_EVENT_TYPES = [
	GET_PROVIDERS_CLIENT,
	GET_PROVIDERS_SERVER,
    BITSWAP_CLIENT,
	BITSWAP_SERVER,
	READ_FROM_FILE_STORE
]
