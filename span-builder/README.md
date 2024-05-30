# Span Creator
The span creator sits between the daemon process and the jaeger backend. It takes the raw, timestamp-based logs and build spans based on the start and end timestamps of the raw logs. The creator then sends these spans to the jaeger backend.

## Goal
The goal for the span creator is two-fold:

1. Build a single span from two logs: log with the starting timestamp, and log with the ending timestamp.

2. Construct parent-child relationships between spans, by creating a __parentSpanId__ field in the child span and set its value to the parent span.


## Input
Raw logs in the format of:
```
<trace-id> <node-id> <thread-id> <timestamp> <human-readable-time> <event-type> <details>
```


## Output
The span creator will post JSON to the jaeger backend. The JSON will be in the format of:
```
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": {
              "stringValue": "nabu-tracer"
            }
          }
        ]
      },
      "scopeSpans": [
        {
          "scope": {
            "name": ${scope-name}
          },
          "spans": [
            {
              "traceId": ${traceId},
              "spanId": ${spanId1},
              "startTimeUnixNano": ${startTime1},
              "endTimeUnixNano": ${endTime1},
              "name": "span 1",
            },
            {
              "traceId": ${traceId},
              "spanId": ${spanId1-1},
              "parentSpanId": ${spanId1},
              "startTimeUnixNano": ${startTime2},
              "endTimeUnixNano": ${endTime2},
              "name": "span 1-1",
            },
            {
              "traceId": ${traceId},
              "spanId": ${spanId2},
              "startTimeUnixNano": ${startTime3},
              "endTimeUnixNano": ${endTime3},
              "name": "span 2",
            },
          ]
        }
      ]
    }
  ]
 }
```
In the example above, the trace contains 2 high-level spans (span 1, span 2), span 1 contains a child span, span 1-1