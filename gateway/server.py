from flask import Flask, request, jsonify, render_template, Response
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import random
from google.cloud import firestore
import time
import threading

app = Flask(__name__)

# Initialize Firestore client
db = firestore.Client()

# List of IPFS node addresses
IPFS_URL = [
    "http://10.128.0.2:5000",  # nabu-0
    "http://10.148.0.2:5000",  # nabu-1
    "http://10.166.0.2:5000",  # nabu-2
    "http://10.198.0.2:5000",  # nabu-3
    "http://10.162.0.2:5000",  # nabu-4
    "http://10.190.0.2:5000",  # nabu-5
    "http://10.208.0.3:5000",  # nabu-6
    "http://10.174.0.2:5000",  # nabu-7
    "http://10.202.0.2:5000",  # nabu-8
    "http://10.200.0.2:5000",  # nabu-9
]

IPFS_NODE_IDX = 0
SAMPLE_RATE = 10 # We will sample (1 / SAMPLE_RATE) of all CIDs for tracing
TIMEOUT_IN_SEC = 15

# GET request timer
TIMER_STARTED = False
START_TIME = None

# Traced requests counter
REQUESTS_TRACED = 0

# IPFS routes
PUT_ROUTE = "/api/v0/block/put"
GET_ROUTE = "/api/v0/block/get"
HEALTH_ROUTE = "/api/v0/healthz"

# Dictionary to store the health status of IPFS nodes
node_health_status = {idx: "Unknown" for idx in range(len(IPFS_URL))}

# Web UI
@app.route("/")
def index():
    return render_template("index.html")


# Forward GET request to IPFS peer to retrieve all content
@app.route("/ipfs", methods=["GET"])
def get_ipfs_content():
    def generate():
        global REQUESTS_TRACED, TIMER_STARTED, START_TIME
        # Retrieve CIDs from Firestore
        cid_records = db.collection("cid").stream()
        cid_list = [record.to_dict()["cid"] for record in cid_records]
        increment_counter("total_requests", len(cid_list))
        # Sample CIDs for tracing
        nsamples = max(1, (len(cid_list) + SAMPLE_RATE - 1) // SAMPLE_RATE)
        traced = random.sample(range(len(cid_list)), nsamples)
        TIMER_STARTED = False
        START_TIME = None
        with ThreadPoolExecutor(max_workers=512) as executor:
            future_to_cid = {
                executor.submit(send_single_get_request, cid, i in traced): cid
                for i, cid in enumerate(cid_list)
            }
            try:
                healthy_node_count = sum(1 for status in node_health_status.values() if status == "Healthy")
                request_timeout = ((len(cid_list) + healthy_node_count - 1) // healthy_node_count) * TIMEOUT_IN_SEC
                for future in as_completed(future_to_cid, timeout=request_timeout):
                    cid = future_to_cid[future]
                    try:
                        status, response, node, trace, time_taken, trace_id = future.result()
                        if trace:
                            REQUESTS_TRACED += 1
                        if status != 200:
                            yield f'data: {{"error": "{response}", "node": "nabu-{node}", "trace": "{trace}", "trace_id": "{trace_id}", "time_taken": "{time_taken:.2f}s"}}\n\n'
                        else:
                            # Escape newlines
                            escaped_response = response.replace("\n", "\\n").replace("\r", "\\r").replace("\"", "\\\"")
                            yield f'data: {{"content": "{escaped_response}", "node": "nabu-{node}", "trace": "{trace}", "trace_id": "{trace_id}", "time_taken": "{time_taken:.2f}s"}}\n\n'
                    except Exception as e:
                        yield f'data: {{"error": "{str(e)}", "node": "nabu-{node}", "trace": "{trace}", "trace_id": "N/A", "time_taken": "N/A"}}\n\n'
            except Exception as e:
                # TimeoutError
                yield f'data: {{"error": "{str(e)}", "node": "N/A", "{False}": "N/A", "trace_id": "N/A", "time_taken": "N/A"}}\n\n'

    return Response(generate(), mimetype="text/event-stream")


# Forward PUT request to IPFS peer
@app.route("/ipfs", methods=["PUT"])
def put_ipfs_content():
    node = get_next_healthy_node()
    if node == -1:
        return jsonify({"error": "No healthy IPFS node found"}), 500

    url = IPFS_URL[node] + PUT_ROUTE
    payload = request.data.decode("utf-8")

    if not url:
        return jsonify({"error": "URL is required"}), 400

    try:
        response = requests.put(url, data=payload)
        response.raise_for_status()
        cid = response.json().get("cid")
        if not cid:
            return jsonify({"error": "Failed to retrieve CID from response"}), 500

        # Store CID to Firestore
        db.collection("cid").add({"cid": cid})

        return jsonify({"content": cid}), response.status_code

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


def get_next_healthy_node():
    global IPFS_NODE_IDX
    next_node = (IPFS_NODE_IDX + 1) % len(IPFS_URL)
    while node_health_status[next_node] != "Healthy":
        if next_node == IPFS_NODE_IDX:
            # No healthy node found
            return -1
        next_node = (next_node + 1) % len(IPFS_URL)
    IPFS_NODE_IDX = next_node
    return next_node


def increment_counter(counter_name, amount):
    counter_ref = db.collection("counters").document(counter_name)
    try:
        counter = counter_ref.get()
        if counter.exists:
            counter_ref.update({"count": firestore.Increment(amount)})
        else:
            counter_ref.set({"count": amount})
    except Exception as e:
        print(f"Error updating counter {counter_name}: {e}")


def send_single_get_request(content, trace):
    global TIMER_STARTED, START_TIME

    node = get_next_healthy_node()
    if node == -1:
        return 500, "No healthy IPFS node found", None, None, None, None

    url = IPFS_URL[node] + GET_ROUTE + f"?cid={content}"

    if trace:
        url += "&trace=1"

    # debug
    print(url)

    if not url:
        return 400, "URL is required", node, trace, None, None

    # Start timer when the first get request is sent to IPFS node
    if not TIMER_STARTED:
        START_TIME = time.time()
        TIMER_STARTED = True

    try:
        response = requests.get(url)
        response.raise_for_status()
        time_taken = time.time() - START_TIME
        trace_id = response.headers.get("Trace-id", "N/A")
        if trace_id == "N/A":
            trace = False
        return response.status_code, response.text, node, trace, time_taken, trace_id

    except requests.RequestException as e:
        time_taken = time.time() - START_TIME
        trace_id = e.response.headers.get("Trace-id", "N/A") if e.response else "N/A"
        if trace_id == "N/A":
            trace = False
        return (
            e.response.status_code if e.response else 500,
            str(e),
            node,
            trace,
            time_taken,
            trace_id,
        )

# Clear the cid collection
@app.route("/clear", methods=["GET"])
def clear_cid_collection():
    try:
        # Retrieve all documents in the cid collection
        cid_records = db.collection("cid").stream()

        # Batch delete all documents
        batch = db.batch()
        for record in cid_records:
            batch.delete(record.reference)
        batch.commit()

        return "Collection cleared successfully", 200

    except Exception as e:
        return str(e), 500

# Check the health of IPFS nodes
@app.route("/ipfs/health", methods=["GET"])
def get_ipfs_health():
    return jsonify(node_health_status)

# Check the health of IPFS nodes in parallel
def check_node_health():
    def check_health(url, idx):
        health_url = url + HEALTH_ROUTE
        try:
            response = requests.get(health_url)
            response.raise_for_status()
            return idx, "Healthy"
        except requests.RequestException:
            return idx, "Unhealthy"

    with ThreadPoolExecutor(max_workers=512) as executor:
        while True:
            print(f"[{time.ctime()}] Sending health check requests")
            futures = {executor.submit(check_health, url, idx): idx for idx, url in enumerate(IPFS_URL)}
            replied_idx = []
            try:
                for future in as_completed(futures, timeout=TIMEOUT_IN_SEC):
                    idx, status = future.result()
                    node_health_status[idx] = status
                    replied_idx.append(idx)
            except Exception as e:
                # TimeoutError
                for idx, _ in enumerate(IPFS_URL):
                    if idx not in replied_idx:
                        node_health_status[idx] = "Unhealthy"
            time.sleep(15)  # Wait 15 more seconds before sending the next round of health check

# Start the health check in a separate thread
threading.Thread(target=check_node_health, daemon=True).start()

def increment_requests_traced():
    global REQUESTS_TRACED
    while True:
        increment_counter("traced_requests", REQUESTS_TRACED)
        REQUESTS_TRACED = 0
        time.sleep(15)

# Start the traced requests counter in a separate thread
threading.Thread(target=increment_requests_traced, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
