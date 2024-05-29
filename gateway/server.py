from flask import Flask, request, jsonify, render_template
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import random
from google.cloud import firestore

app = Flask(__name__)

# Initialize Firestore client
db = firestore.Client()

# List of IPFS node addresses
IPFS_URL = [
    'http://localhost:5001',
    # TODO: Add more IPFS node addresses
]

# IPFS routes
PUT_ROUTE = '/api/v0/block/put'
GET_ROUTE = '/api/v0/block/get'

# Web UI
@app.route('/')
def index():
    return render_template('index.html')

# Forward GET request to IPFS peer to retrieve all content
@app.route('/ipfs', methods=['GET'])
def get_ipfs_content():
    # Retrieve CIDs from Firestore
    cid_records = db.collection('cid').stream()
    cid_list = [record.to_dict()['cid'] for record in cid_records]
    # Sample CIDs for tracing
    nsamples = max(1, len(cid_list) // 10)
    traced = random.sample(range(len(cid_list)), nsamples)

    responses = []

    with ThreadPoolExecutor() as executor:
        future_to_cid = {executor.submit(send_single_get_request, cid, i in traced): cid for i, cid in enumerate(cid_list)}
        for future in as_completed(future_to_cid):
            cid = future_to_cid[future]
            try:
                status, response = future.result()
                if status != 200:
                    responses.append({'error': response})
                else:
                    responses.append({'content': response})
            except Exception as e:
                responses.append({'error': str(e)})

    return jsonify({"content": responses})

# Forward PUT request to IPFS peer
@app.route('/ipfs', methods=['PUT'])
def put_ipfs_content():
    # TODO: use round-robin to choose the peer
    ipfs_node_idx = 0
    url = IPFS_URL[ipfs_node_idx] + PUT_ROUTE
    # TODO: support other input formats
    payload = request.data.decode('utf-8')

    if not url:
        return jsonify({'error': 'URL is required'}), 400

    try:
        response = requests.put(url, data=payload)
        response.raise_for_status()
        cid = response.json().get('cid')
        if not cid:
            return jsonify({'error': 'Failed to retrieve CID from response'}), 500

        # Store CID to Firestore
        db.collection('cid').add({'cid': cid})

        return jsonify({
            'content': cid
        }), response.status_code

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500

def send_single_get_request(content, trace=False):
    ipfs_node_idx = 0
    url = IPFS_URL[ipfs_node_idx] + GET_ROUTE + f"?cid={content}"
    if trace:
        url += "&trace=1"

    if not url:
        return 400, 'URL is required'

    try:
        response = requests.get(url)
        response.raise_for_status()
        # TODO: reply with bytestream instead of text
        return response.status_code, response.text

    except requests.RequestException as e:
        return e.response.status_code if e.response else 500, str(e)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
