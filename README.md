# perf-mq-producer

## prometheus interface
```bash
curl localhost:20002/actuator/prometheus
```

## start skywalking

configure environment:

`SW_AGENT_ENABLE`: open skywalking agent, switch `true`, `false`, default `false`.

`SW_SERVICE_NAME`: skywalking trace service name, default `perf-mq-producer`.

`SW_COLLECTOR_URL`: skywalking collector url, default `localhost:11800`.
