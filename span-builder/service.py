from typing import Optional, List
from flask import Flask, request, jsonify
import threading
import requests
import copy
from collections import defaultdict
import logging
from logging.handlers import TimedRotatingFileHandler
import hashlib
from constants import *
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    'app.log',
    when='H',
    interval=4,
    backupCount=7
)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.addHandler(handler)

app = Flask(__name__)

data_store = {}
data_store_locks = defaultdict(threading.Lock)
spans_sent = []


class JaegerPostError(Exception):
    pass


def is_trace_complete_v2(trace_id: str) -> bool:
    trace = data_store[trace_id]

    for span, status in trace.items():
        # Check if there is not a start and end. Should add extra validation besides length.
        if len(status) != 2:
            return False

    return True



class Span:
    def __init__(self, span_id: str ,node_id: str, type: str, start_time: int, end_time: int, peer_node_id: str, parent_id: str):
        self.span_id = ""
        self.node_id = node_id
        self.peer_node_id = peer_node_id
        self.type = type
        self.start_time = start_time * 1_000_000
        self.end_time = end_time * 1_000_000
        self.parent_id = parent_id
        self.children = children if children is not None else []


def construct_span_id_from_span(trace_id, node_id, peer_node_id, span_name):
    span_id = f"{trace_id}_{node_id}_{peer_node_id}_{span_name}"
    span_id = hashlib.md5(span_id.encode('utf-8')).hexdigest()[:16]
    return span_id


def build_parent_child_spans(trace_id: str):
    spans = []

    def find_span(node_id: str, type: str):
        for i, span in enumerate(spans):
            if (span.node_id == node_id and span.type == type):
                return i

        return -1

    trace = data_store[trace_id]["data"]

    # Stage is either 'start' or 'end'
    for k, stages in trace.items():
        if len(stages) < 2:
          return None

        # If 'start' and 'end' are not both present, skip
        start, end = stages.values()

        node_id, peer_node_id, _type = k

        span = Span(
            span_id = construct_span_id_from_span(trace_id, node_id, peer_node_id, _type),
            node_id=node_id,
            type=_type,
            start_time=start,
            end_time=end,
            peer_node_id=peer_node_id,
            parent_id=None,
        )

        spans.append(span)

    span_set = set()

    for i, span in enumerate(spans):
        span_set.add(span.type)
        if span.type == GET_PROVIDERS_CLIENT:
            maybe_child_index = find_span(span.peer_node_id, GET_PROVIDERS_SERVER)

            if maybe_child_index == -1:
                return None

        elif span.type == GET_PROVIDERS_SERVER:
            maybe_parent_index = find_span(span.peer_node_id, GET_PROVIDERS_CLIENT)

            if maybe_parent_index != -1:
                parent_id = spans[maybe_parent_index].span_id
                span.parent_id = parent_id
            else:
                return None
        elif span.type == BITSWAP_CLIENT:
            maybe_child_index = find_span(span.peer_node_id, BITSWAP_SERVER)

            if maybe_child_index == -1:
              return None
        elif span.type == BITSWAP_SERVER:
            maybe_parent_index = find_span(span.peer_node_id, BITSWAP_CLIENT)

            if maybe_parent_index != -1:
                parent_id = spans[maybe_parent_index].span_id
                span.parent_id = parent_id
            else:
                return None

            maybe_child_index = find_span(span.node_id, READ_FROM_FILE_STORE)

            if maybe_child_index == -1:
                return None
        elif span.type == READ_FROM_FILE_STORE:
            maybe_parent_index = find_span(span.node_id, BITSWAP_SERVER)

            if maybe_parent_index >= 0:
                parent_id = spans[maybe_parent_index].span_id
                span.parent_id = parent_id
            else:
                return None

    if len(span_set) < 5:
      return None

    print("Trace is complete!")
    print_spans(spans)
    return spans


def print_spans(spans, prefix='', is_tail=True):
    for i, span in enumerate(spans):
        is_last = i == (len(spans) - 1)
        connector = '└── ' if is_last else '├── '
        child_prefix = '    ' if is_last else '│   '

        print(f"{prefix}{connector}Node id: {span.node_id}")
        print(f"{prefix}{child_prefix}Peer node id: {span.peer_node_id}")
        print(f"{prefix}{child_prefix}Event: {span.type}")
        print(f"{prefix}{child_prefix}Start: {span.start_time}")
        print(f"{prefix}{child_prefix}End: {span.end_time}")

        if span.children:
            new_prefix = prefix + child_prefix


def send_trace_to_jaeger(payload):
    trace_id = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0][JAEGER_TRACE_ID_KEY]
    print(f"Sending payload to Jaeger, trace ID: {trace_id}")
    try:
        resp = requests.post(JAEGER_ENDPOINT, json=payload)

        print(resp.text)
    except:
        raise JaegerPostError("Failed to post to Jaeger endpoint")


@app.route("/v3/buildspan", methods=["POST"])
def build_span_v3():
    content = request.get_json()
    trace_id = content[RAW_LOG_TRACE_ID_KEY]
    node_id = content[RAW_LOG_NODE_ID_KEY]
    peer_node_id = content[RAW_LOG_PEER_NODE_ID_KEY]
    timestamp = int(content[RAW_LOG_TIME_STAMP_KEY])
    span_name, stage = _get_func_name_and_stage(content)

    human_timestamp = datetime.fromtimestamp(timestamp/1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"Received trace event from {request.remote_addr} at {human_timestamp}: {trace_id}, node {node_id}, thread N/A, {span_name}_{stage} {stage}")

    if trace_id not in data_store:
        data_store[trace_id] = {"creation": datetime.now(), "data": {}}
    trace = data_store[trace_id]["data"]

    key = (node_id, peer_node_id, span_name)

    span = trace.setdefault(key, {})
    span[stage] = timestamp
    print(f"Setting {key} {stage} to {timestamp}")

    spans = build_parent_child_spans(trace_id)

    if spans and len(spans):
        try:
            for span in spans:
                span_id = construct_span_id_from_span(
                    trace_id=trace_id,
                    node_id=span.node_id,
                    peer_node_id=span.peer_node_id,
                    span_name=span.type
                )

                if span_id in spans_sent:
                    continue

                payload = copy.deepcopy(STARTER_SPAN)

                span_payload = {
                    JAEGER_TRACE_ID_KEY: trace_id,
                    JAEGER_SPAN_ID_KEY: span_id,
                    JAEGER_PARENT_SPAN_ID_KEY: span.parent_id,
                    JAEGER_START_TIME_NANO_KEY: span.start_time,
                    JAEGER_END_TIME_NANO_KEY: span.end_time,
                    JAEGER_SPAN_OPERATION_NAME_KEY: f"{span.type}_{span.node_id}",
                    JAEGER_SPAN_KIND_KEY: 2,
                }
                payload["resourceSpans"][0]["scopeSpans"][0]["spans"].append(span_payload)

                send_trace_to_jaeger(payload)

                # Keep the most recent 1000 sent spans to prevent memory leak
                spans_sent.append(span_id)
                if len(spans_sent) > 10000:
                    spans_sent.pop(0)

            # Keep a trace for at most 2 minutes to prevent memory leak
            if datetime.now() - data_store[trace_id]["creation"] >= timedelta(minutes=2):
                del data_store[trace_id]
        except Exception as exc:
            return jsonify({ 'error': str(exc)}), 500

    return jsonify(), 200


def _extract_event_info(event: str):
    return event.rsplit('_', 1)

def _get_func_name_and_stage(content):
    event_type = content[RAW_LOG_EVENT_TYPE_KEY]

    assert event_type.endswith(Stage.START.name) or event_type.endswith(Stage.END.name)

    return _extract_event_info(event_type)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5200)
