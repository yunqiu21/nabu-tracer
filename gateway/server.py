from flask import Flask, request, jsonify, render_template, Response
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import random
from google.cloud import firestore
import time

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

IPFS_PUT_NODE = 6  # Hardcoded for now to a responsive node.
IPFS_GET_NODE = 0  # Update with each GET request

# IPFS routes
PUT_ROUTE = "/api/v0/block/put"
GET_ROUTE = "/api/v0/block/get"


# Web UI
@app.route("/")
def index():
    return render_template("index.html")


# Forward GET request to IPFS peer to retrieve all content
@app.route("/ipfs", methods=["GET"])
def get_ipfs_content():
    def generate():
        # Retrieve CIDs from Firestore
        cid_records = db.collection("cid").stream()
        cid_list = [record.to_dict()["cid"] for record in cid_records]
        # Sample CIDs for tracing
        nsamples = max(1, len(cid_list) // 10)
        traced = random.sample(range(len(cid_list)), nsamples)

        with ThreadPoolExecutor() as executor:
            future_to_cid = {
                executor.submit(send_single_get_request, cid, i in traced): cid
                for i, cid in enumerate(cid_list)
            }
            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                try:
                    status, response, node, trace, time_taken = future.result()
                    if status != 200:
                        yield f'data: {{"error": "{response}", "node": "nabu-{node}", "trace": "{trace}", "time_taken": "{time_taken:.2f}s"}}\n\n'
                    else:
                        yield f'data: {{"content": "{response}", "node": "nabu-{node}", "trace": "{trace}", "time_taken": "{time_taken:.2f}s"}}\n\n'
                except Exception as e:
                    yield f'data: {{"error": "{str(e)}", "node": "nabu-{node}", "trace": "{trace}", "time_taken": "N/A"}}\n\n'

    return Response(generate(), mimetype="text/event-stream")


# Forward PUT request to IPFS peer
@app.route("/ipfs", methods=["PUT"])
def put_ipfs_content():
    # TODO: For PUT request, maybe can try all nodes until one succeeds?
    url = IPFS_URL[IPFS_PUT_NODE] + PUT_ROUTE
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


def send_single_get_request(content, trace=False):
    global IPFS_GET_NODE
    node = IPFS_GET_NODE
    IPFS_GET_NODE = (IPFS_GET_NODE + 1) % len(IPFS_URL)
    url = IPFS_URL[node] + GET_ROUTE + f"?cid={content}"

    if trace:
        url += "&trace=1"
        # TODO: mark the CID as sampled in datastore

    # debug
    print(url)

    if not url:
        return 400, "URL is required", None, None, None

    start_time = time.time()
    try:
        response = requests.get(url)
        response.raise_for_status()
        time_taken = time.time() - start_time
        return response.status_code, response.text, node, trace, time_taken

    except requests.RequestException as e:
        time_taken = time.time() - start_time
        return (
            e.response.status_code if e.response else 500,
            str(e),
            node,
            trace,
            time_taken,
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=80)
