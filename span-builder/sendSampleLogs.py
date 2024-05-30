import requests
import time
from datetime import datetime, timedelta
import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str)
    args = parser.parse_args()

    traceId = os.urandom(16).hex()
    current_time = datetime.now()
    timestamp1 = current_time - timedelta(seconds=4)
    span1 = os.urandom(8).hex()
    payload1 = {
        "traceId": traceId,
        "spanId": span1,
        "nodeId": "node1",
        "threadId": "thread1",
        "timestamp": int(timestamp1.timestamp() * 1e9),
        "eventType": "func1_START",
    }
    requests.post(args.endpoint, json=payload1)

    timestamp2 = current_time - timedelta(seconds=3)
    payload2 = {
        "traceId": traceId,
        "spanId": span1,
        "nodeId": "node1",
        "threadId": "thread1",
        "timestamp": int(timestamp2.timestamp() * 1e9),
        "eventType": "func1_STOP",
    }
    requests.post(args.endpoint, json=payload2)

    span2 = os.urandom(8).hex()
    timestamp3 = current_time - timedelta(seconds=2)
    payload3 = {
        "traceId": traceId,
        "spanId": span2,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp3.timestamp() * 1e9),
        "eventType": "func2_START",
    }
    requests.post(args.endpoint, json=payload3)

    timestamp4 = current_time - timedelta(seconds=1)
    payload4 = {
        "traceId": traceId,
        "spanId": span2,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp4.timestamp() * 1e9),
        "eventType": "func2_STOP",
    }
    requests.post(args.endpoint, json=payload4)

    span3 = os.urandom(8).hex()
    timestamp5 = current_time - timedelta(milliseconds=1800)
    payload5 = {
        "traceId": traceId,
        "spanId": span3,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp5.timestamp() * 1e9),
        "eventType": "func2-2_START",
    }
    requests.post(args.endpoint, json=payload5)

    timestamp6 = current_time - timedelta(milliseconds=1200)
    payload6 = {
        "traceId": traceId,
        "spanId": span3,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp6.timestamp() * 1e9),
        "eventType": "func2-2_STOP",
    }
    requests.post(args.endpoint, json=payload6)


if __name__ == "__main__":
    main()
