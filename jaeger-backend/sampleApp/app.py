
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', type=str)
    parser.add_argument('--service', type=str)
    parser.add_argument('--span', type=str)
    args = parser.parse_args()

    resource = Resource(attributes={
        SERVICE_NAME: f"{args.service}"
    })

    traceProvider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"http://{args.endpoint}/v1/traces"))
    traceProvider.add_span_processor(processor)
    trace.set_tracer_provider(traceProvider)
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(f"{args.span}"):
        wait_and_print("First inner function")
        wait_and_print("Second inner function")
    
def wait_and_print(s: str):
    time.sleep(1)
    print(s)

if __name__ == "__main__":
    main()