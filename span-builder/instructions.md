To run:

1. start a local Jaeger instance by:
```
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.57
```

2. run `pip install -r requirements.txt`

3. run `python service.py`. This will start the span builder service at http://localhost:5200, and it will post traces to the above jaeger instance at http://localhost:4318

4. run `python sendSampleLogs.py`. This sends sample request to the span builder service.

5. view the processed traces at http://localhost:16686/