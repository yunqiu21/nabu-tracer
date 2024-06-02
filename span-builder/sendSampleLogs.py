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
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span1,
        "nodeId": "node1",
        "threadId": "thread1",
        "timestamp": int(timestamp1.timestamp() * 1e9),
        "eventType": "GET_PROVIDERS_SERVER_START",
        "peerNodeId": "node2"
    }
    requests.post(args.endpoint, json=payload1)

    timestamp2 = current_time - timedelta(seconds=3)
    payload2 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span1,
        "nodeId": "node1",
        "threadId": "thread1",
        "timestamp": int(timestamp2.timestamp() * 1e9),
        "eventType": "GET_PROVIDERS_SERVER_END",
        "peerNodeId": "node2"
    }
    requests.post(args.endpoint, json=payload2)

    span2 = os.urandom(8).hex()
    timestamp3 = current_time - timedelta(seconds=2)
    payload3 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span2,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp3.timestamp() * 1e9),
        "eventType": "GET_PROVIDERS_CLIENT_START",
        "peerNodeId": "node1"
    }
    requests.post(args.endpoint, json=payload3)

    timestamp4 = current_time - timedelta(seconds=1)
    payload4 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span2,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp4.timestamp() * 1e9),
        "eventType": "GET_PROVIDERS_CLIENT_END",
        "peerNodeId": "node3"
    }
    requests.post(args.endpoint, json=payload4)

    span3 = os.urandom(8).hex()
    timestamp5 = current_time - timedelta(milliseconds=1800)
    payload5 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span3,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp5.timestamp() * 1e9),
        "eventType": "BITSWAP_CLIENT_START",
        "peerNodeId": "node3"
    }
    requests.post(args.endpoint, json=payload5)

    timestamp6 = current_time - timedelta(milliseconds=1200)
    payload6 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "spanId": span3,
        "nodeId": "node2",
        "threadId": "thread1",
        "timestamp": int(timestamp6.timestamp() * 1e9),
        "eventType": "BITSWAP_CLIENT_END",
        "peerNodeId": "node3"
    }

    requests.post(args.endpoint, json=payload6)

    timestamp7 = current_time - timedelta(seconds=2)
    payload7 = {
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "nodeId": "node3",
        "eventType": "BITSWAP_SERVER_START",
        "threadId": "thread4",
        "timestamp": int(timestamp7.timestamp() * 1e9),
        "peerNodeId": "node2"


    }
    requests.post(args.endpoint, json=payload7)
    timestamp8 = current_time - timedelta(seconds=1)

    payload8 = {
        "nodeId": "node3",
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "eventType": "READ_FROM_FILE_STORE_START",
        "threadId": "thread6",
        "timestamp": int(timestamp8.timestamp() * 1e9),
        "peerNodeId": ""
    }
    requests.post(args.endpoint, json=payload8)

    timestamp9 = current_time - timedelta(seconds=1.5)

    payload9 = {
        "nodeId": "node3",
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "eventType": "READ_FROM_FILE_STORE_END",
        "threadId": "thread6",
        "timestamp": int(timestamp9.timestamp() * 1e9),
        "peerNodeId": ""
    }
    requests.post(args.endpoint, json=payload9)

    timestamp10 = current_time - timedelta(seconds=.75)
    payload10 = {
        "nodeId": "node3",
        "traceId": "0f9c663e-cfe1-4934-968e-417764d57395",
        "eventType": "BITSWAP_SERVER_END",
        "threadId": "thread4",
        "peerNodeId": "node2",
        "timestamp": int(timestamp10.timestamp() * 1e9),
    } 
    requests.post(args.endpoint, json=payload10)


if __name__ == "__main__":
    main()
