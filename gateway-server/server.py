from flask import Flask, request, jsonify, render_template
import requests
# TODO: from kazoo.client import KazooClient
from pymongo import MongoClient
import random

app = Flask(__name__)

# TODO: Add Zookeeper functionality

mongo_uri = '<hidden-for-pull-request>'
client = MongoClient(mongo_uri)
db = client['nabu']

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
    # Retrieve CIDs from database
    cid_records = db['cid'].find()
    cid_list = [record['cid'] for record in cid_records]
    # Sample CIDs for tracing
    nsamples = max(1, len(cid_list) // 10)
    traced = random.sample(range(len(cid_list)), nsamples)

    responses = []

    for i, cid in enumerate(cid_list):
        responses.append(send_single_get_request(cid, i in traced))

    # TODO: handle errors
    return jsonify({"status_code": 200, "content": responses})

# Forward PUT request to IPFS peer
@app.route('/ipfs', methods=['PUT'])
def put_ipfs_content():
    # TODO: use round-robin to choose the peer
    ipfs_node_idx = 0
    url = IPFS_URL[ipfs_node_idx] + PUT_ROUTE
    # TODO: support other input format
    payload = request.data.decode('utf-8')

    if not url:
        return jsonify({'error': 'URL is required'}), 400

    try:
        response = requests.put(url, data=payload)
        cid = response.json().get('cid')
        # Store CID to database
        result = db['cid'].insert_one({'cid': cid})

        return jsonify({
            'status_code': response.status_code,
            'content': cid
        })

    except requests.RequestException as e:
        return jsonify({'error': str(e)}), 500

def send_single_get_request(content, trace = False):
    ipfs_node_idx = 0
    url = IPFS_URL[ipfs_node_idx] + GET_ROUTE + f"?cid={content}"
    if trace:
        url += "&trace=1"

    if not url:
        return 'URL is required'

    try:
        response = requests.get(url)
        return response.text

    except requests.RequestException as e:
        return str(e)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
