from flask import Flask, request, jsonify
import hashlib
import json
import logging
import threading
import requests
import copy
from collections import defaultdict
import os
from constants import *

app = Flask(__name__)

data_store = {}
data_store_locks = defaultdict(threading.Lock)


class JaegerPostError(Exception):
    pass


class LogStorageError(Exception):
    pass


class TraceConstructionError(Exception):
    pass


class AddLineageError(Exception):
    pass

def is_trace_complete_v2(trace_id: str) -> bool:
    trace = data_store[trace_id]

    for span, status in trace.items():
        # Check if there is not a start and end. Should add extra validation besides length.
        if len(status) != 2:
            return False

    return True
    

@app.route("/v3/buildspan", methods=["POST"])
def build_span_v3():
    content = request.get_json()

    trace_id = content[RAW_LOG_TRACE_ID_KEY]
    node_id = content[RAW_LOG_NODE_ID_KEY]
    thread_id = content[RAW_LOG_THREAD_ID_KEY]
    timestamp = int(content[RAW_LOG_TIME_STAMP_KEY])
    span_name, stage = _get_func_name_and_stage(content)

    logging.info(f"Received trace event: {trace_id}, {thread_id}, {span_name} {stage}")

    trace = data_store.setdefault(trace_id, {})
    lock = data_store_locks.setdefault(trace_id,threading.Lock())

    with lock:
        try:
            key = (node_id, thread_id, span_name)

            span = trace.setdefault(key, {})
            span[stage] = timestamp

            if is_trace_complete_v2(trace_id):
                # This sends to Jaeger one by one - can we batch send these?
                for key, value in trace.items():
                    start, end = value.values()
                    node_id, thread_id, span_name = key

                    retval = copy.deepcopy(STARTER_SPAN)
                    span_id = f"{trace_id}_{node_id}_{thread_id}_{span_name}_{stage}"
                    span_id = hashlib.md5(span_id.encode('utf-8')).hexdigest()[:16]

                    span = {
                        JAEGER_TRACE_ID_KEY: trace_id,
                        JAEGER_SPAN_ID_KEY: span_id,
                        JAEGER_PARENT_SPAN_ID_KEY: "",
                        JAEGER_START_TIME_NANO_KEY: start,
                        JAEGER_END_TIME_NANO_KEY: end,
                        JAEGER_SPAN_OPERATION_NAME_KEY: span_name,
                        JAEGER_SPAN_KIND_KEY: 2,
                    }
                    retval["resourceSpans"][0]["scopeSpans"][0]["spans"].append(span)

                    send_trace_to_jaeger(retval)
                # Remove from storage
                del data_store[trace_id]
        except Exception as exc:
            logging.exception(exc)
            return jsonify({ 'error': str(exc)}), 500

    return jsonify(), 200

@app.route("/v1/buildspan", methods=["POST"])
def build_span():
    if request.is_json:
        
        content = request.get_json()
        val_trace_id = content[RAW_LOG_TRACE_ID_KEY]
        val_node_id = content[RAW_LOG_NODE_ID_KEY]
        val_thread_id = content[RAW_LOG_THREAD_ID_KEY]
        val_span_name, val_stage = _get_func_name_and_stage(content)

        try:
            add_raw_log_to_data_store(
                val_trace_id,
                val_node_id,
                val_thread_id,
                val_span_name,
                val_stage,
                content,
            )
            if is_span_complete(val_trace_id, val_node_id, val_thread_id, val_span_name):
                payload = construct_trace_object_with_single_span(val_trace_id, val_node_id, val_thread_id, val_span_name)
                send_trace_to_jaeger(payload)
                delete_raw_log_from_data_store(val_trace_id, val_node_id, val_thread_id, val_span_name)
            return (
                jsonify({"message": "JSON received successfully", "data": content}),
                200,
            )
        except (
            LogStorageError,
            TraceConstructionError,
            AddLineageError,
            JaegerPostError,
        ) as e:
            return jsonify({"message": str(e), "data": content}), 400
        except Exception as e:
            return (
                jsonify(
                    {"message": "Other Error in building the span", "data": content}
                ),
                400,
            )
    else:
        return jsonify({"error": "Request must be JSON"}), 400


def add_raw_log_to_data_store(
    val_trace_id, val_node_id, val_thread_id, val_span_name, val_stage, content
):
    # Add raw log to data_store, indexed by val_trace_id, val_node_id, val_thread_id, val_span_name, val_stage

    try:
        if val_trace_id not in data_store_locks:
            data_store_locks[val_trace_id] = threading.Lock()
        with data_store_locks[val_trace_id]:
            span_storage = _get_storage_for_raw_log(
                val_trace_id, val_node_id, val_thread_id, val_span_name
            )
            _add_raw_log_to_storage(content, span_storage, val_stage)
    except:
        raise LogStorageError("Failed to add log to storage")


def delete_raw_log_from_data_store(
    val_trace_id, val_node_id, val_thread_id, val_span_name
):
    try:
        if val_trace_id not in data_store_locks:
            data_store_locks[val_trace_id] = threading.Lock()
        with data_store_locks[val_trace_id]:
            del data_store[val_trace_id][val_node_id][val_thread_id][val_span_name]
    except:
        raise LogStorageError("Failed to delete log from storage")


def send_trace_to_jaeger(payload):
    print("Sending payload to Jaeger: ", payload)
    try:
        requests.post(JAEGER_ENDPOINT, json=payload)
    except:
        raise JaegerPostError("Failed to post to Jaeger endpoint")


#
# V1 helper functions
#

def is_span_complete(val_trace_id, val_node_id, val_thread_id, val_span_name):
    with data_store_locks[val_trace_id]:
        if (
            Stage.START.name in data_store[val_trace_id][val_node_id][val_thread_id][
                val_span_name
            ]
            and Stage.STOP.name in data_store[val_trace_id][val_node_id][val_thread_id][
                val_span_name
            ]
        ):
            return True
    return False


def construct_trace_object_with_single_span(val_trace_id, val_node_id, val_thread_id, val_span_name):
    try:
        payload = copy.deepcopy(STARTER_SPAN)
        with data_store_locks[val_trace_id]:
            span_start_log = data_store[val_trace_id][val_node_id][
                val_thread_id
            ][val_span_name][Stage.START.name]
            span_stop_log = data_store[val_trace_id][val_node_id][
                val_thread_id
            ][val_span_name][Stage.STOP.name]
            span = _construct_span_object_from_log(
                span_start_log, span_stop_log
            )
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"].append(
                span
            )
        # TODO: We should also delete the raw logs after we deem it complete and construct a trace object from it
        return payload
    except:
        raise TraceConstructionError("Failed to construct trace from raw logs")


@app.route("/v2/buildspan", methods=["POST"])
def build_span_v2():
    if request.is_json:
        content = request.get_json()
        val_trace_id = content[RAW_LOG_TRACE_ID_KEY]
        val_node_id = content[RAW_LOG_NODE_ID_KEY]
        val_thread_id = content[RAW_LOG_THREAD_ID_KEY]
        val_span_name, val_stage = _get_func_name_and_stage(content)

        try:
            add_raw_log_to_data_store(
                val_trace_id,
                val_node_id,
                val_thread_id,
                val_span_name,
                val_stage,
                content,
            )
            if is_trace_complete(val_trace_id):
                payload = construct_trace_object(val_trace_id)
                payload = add_parent_child_relationship_to_constructed_trace(payload)
                send_trace_to_jaeger(payload)
            return (
                jsonify({"message": "JSON received successfully", "data": content}),
                200,
            )
        except (
            LogStorageError,
            TraceConstructionError,
            AddLineageError,
            JaegerPostError,
        ) as e:
            return jsonify({"message": str(e), "data": content}), 400
        except Exception as e:
            return (
                jsonify(
                    {"message": "Other Error in building the span", "data": content}
                ),
                400,
            )
    else:
        return jsonify({"error": "Request must be JSON"}), 400


#
# V2 helper APIs, not used in V1
#

def is_trace_complete(val_trace_id):
    # TODO: determine when a trace is complete
    # criteria: function START should be closed by a function END
    #
    # However, here's a counter example:
    # Let's say we have trace1, comprising of span1 and span2,
    # which are non-overlapping in time.
    # We can have children like span1-1, span1-2, whatever.
    # When the span builder receives only span1-1, span1-2, and span1,
    # what makes the span builder wait for span2?
    # Because it can just send trace1->span1->{span1-1, span1-2} and that would be a valid trace
    # In this case we'll be missing span2 information

    with data_store_locks[val_trace_id]:
        for val_node_id in data_store[val_trace_id]:
            for val_thread_id in data_store[val_trace_id][val_node_id]:
                for val_span_name in data_store[val_trace_id][val_node_id][
                    val_thread_id
                ]:
                    if (
                        Stage.START.name
                        not in data_store[val_trace_id][val_node_id][val_thread_id][
                            val_span_name
                        ]
                        or Stage.STOP.name
                        not in data_store[val_trace_id][val_node_id][val_thread_id][
                            val_span_name
                        ]
                    ):
                        return False
    return True


def construct_trace_object(val_trace_id):
    try:
        payload = copy.deepcopy(STARTER_SPAN)
        with data_store_locks[val_trace_id]:
            for val_node_id in data_store[val_trace_id]:
                for val_thread_id in data_store[val_trace_id][val_node_id]:
                    for val_span_name in data_store[val_trace_id][val_node_id][
                        val_thread_id
                    ]:
                        span_start_log = data_store[val_trace_id][val_node_id][
                            val_thread_id
                        ][val_span_name][Stage.START.name]
                        span_stop_log = data_store[val_trace_id][val_node_id][
                            val_thread_id
                        ][val_span_name][Stage.STOP.name]
                        span = _construct_span_object_from_log(
                            span_start_log, span_stop_log
                        )
                        payload["resourceSpans"][0]["scopeSpans"][0]["spans"].append(
                            span
                        )
        # TODO: We should also delete the raw logs after we deem it complete and construct a trace object from it
        return payload
    except:
        raise TraceConstructionError("Failed to construct trace from raw logs")


def add_parent_child_relationship_to_constructed_trace(payload):
    # Parent criteria:
    # 1. parent's starttime <= child's starttime, and, parent's endtime >= child's endtime
    # 2. parent's span should be the tightest span that satisfies criteria 1

    try:
        spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        for span in spans:
            parent_starttime = float("-inf")
            parent_endtime = float("inf")
            for other in spans:
                if span[JAEGER_SPAN_ID_KEY] == other[JAEGER_SPAN_ID_KEY]:
                    continue
                if int(other[JAEGER_START_TIME_NANO_KEY]) <= int(
                    span[JAEGER_START_TIME_NANO_KEY]
                ) and int(other[JAEGER_END_TIME_NANO_KEY]) >= int(
                    span[JAEGER_END_TIME_NANO_KEY]
                ):
                    if (
                        int(other[JAEGER_START_TIME_NANO_KEY]) > parent_starttime
                        and int(other[JAEGER_END_TIME_NANO_KEY]) < parent_endtime
                    ):
                        span[JAEGER_PARENT_SPAN_ID_KEY] = other[JAEGER_SPAN_ID_KEY]
                        parent_starttime = int(other[JAEGER_START_TIME_NANO_KEY])
                        parent_endtime = int(other[JAEGER_END_TIME_NANO_KEY])
        return payload
    except:
        raise AddLineageError("Failed to build parent-child relationship")



def _add_raw_log_to_storage(content, storage, index):
    storage[index] = content


def _get_storage_for_raw_log(val_trace_id, val_node_id, val_thread_id, val_span_name):
    # Create storage (dict) for keys that doesn't currently exist

    if val_trace_id not in data_store:
        data_store[val_trace_id] = {}
    if val_node_id not in data_store[val_trace_id]:
        data_store[val_trace_id][val_node_id] = {}
    if val_thread_id not in data_store[val_trace_id][val_node_id]:
        data_store[val_trace_id][val_node_id][val_thread_id] = {}
    if val_span_name not in data_store[val_trace_id][val_node_id][val_thread_id]:
        data_store[val_trace_id][val_node_id][val_thread_id][val_span_name] = {}
    return data_store[val_trace_id][val_node_id][val_thread_id][val_span_name]


def _construct_span_object_from_log(span_start_log, span_stop_log):
    event_type = span_start_log[RAW_LOG_EVENT_TYPE_KEY]
    span_name = event_type[: event_type.find(Stage.START.name)]
    span = {
        # TODO: traceId needs to be 16-byte hex: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto#L88
        JAEGER_TRACE_ID_KEY: span_start_log[RAW_LOG_TRACE_ID_KEY],
        JAEGER_SPAN_ID_KEY: span_start_log[RAW_LOG_SPAN_ID_KEY],
        JAEGER_PARENT_SPAN_ID_KEY: "",
        JAEGER_START_TIME_NANO_KEY: span_start_log[
            RAW_LOG_TIME_STAMP_KEY
        ],  # TODO: check ms/ns
        JAEGER_END_TIME_NANO_KEY: span_stop_log[RAW_LOG_TIME_STAMP_KEY],
        JAEGER_SPAN_OPERATION_NAME_KEY: span_name,
        JAEGER_SPAN_KIND_KEY: 2,
    }
    return span


def _get_func_name_and_stage(content):
    val_event_type = content[RAW_LOG_EVENT_TYPE_KEY]
    if Stage.START.name in val_event_type:
        return val_event_type[: val_event_type.find(Stage.START.name)], Stage.START.name
    elif Stage.STOP.name in val_event_type:
        return val_event_type[: val_event_type.find(Stage.STOP.name)], Stage.STOP.name


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5200)
