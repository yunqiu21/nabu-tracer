# Nabu-Tracer

Nabu-Tracer is a distributed tracing system tailored specifically for private IPFS networks. This repository aims to bridge the gap in tracing techniques within decentralized architectures by providing a comprehensive solution for profiling requests in a P2P environment.

## Project Scope

Nabu-Tracer sets out to investigate profiling techniques within private IPFS networks. Our goal is to develop a robust system with the following core functionalities:

### 1. Request Sampling Policy

Establishing a strategy to select a representative subset of requests originating from IPFS nodes for profiling is crucial. This policy will ensure that our tracing system captures a diverse range of requests for analysis without overwhelming the infrastructure.

### 2. Event Capture

Nabu-Tracer will capture informative events occurring throughout the lifecycle of a sampled request. These events include but are not limited to:

- The initial client request
- External requests made during bitswap lookups (both sent and received)
- Key points during the DHT walk process
- Content retrieval from the content store
- The final response delivered back to the client

### 3. Log Persistence

Captured event logs will be persistently stored to facilitate comprehensive analysis. Ensuring the durability of these logs is essential for post-mortem investigations and performance optimizations.

### 4. Trace Analytics

To provide actionable insights, Nabu-Tracer will feature a user-friendly interface for visualizing traces as spans. Basic search and data analysis capabilities will empower users to identify patterns, anomalies, and potential bottlenecks within the P2P network. Nabu-Tracer uses [Jaeger](https://www.jaegertracing.io/), an open-source trace collector and span query engine, to priode such analytics capability.

<!-- TODO: add installation and usage guides -->

