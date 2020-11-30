## flink-metrics-kafka

## 配置
| 配置项| 默认值| 必填| 例| 说明 |
| --- | --- | ---| ---| --- |
| bootstrap.servers| | 是 | localhost:9092| kafka地址 |
| topic| flink-metrics | 否 | | kafka主题 |
| kdyBy| | 否 | host | kafka key属性名 |
| acks| all | 否 |  |  kafka配置acks| 
| retries| 0 | 否 |  |  kafka配置retries| 
| batch.size| 16384 | 否 |  |  kafka配置batch.size| 
| linger.ms| 1 | 否 |  |  kafka配置linger.ms| 
| buffer.memory| 33554432 | 否 |  |  kafka配置buffer.memory|


## 例
```
metrics.reporters: kafka
metrics.reporter.kafka.class: org.apache.flink.metrics.kafka.KafkaReporter
metrics.reporter.kafka.bootstrap.servers: localhost:9092
metrics.reporter.kafka.topic: flink-metrics
metrics.reporter.kafka.keyBy: task_attempt_id
metrics.reporter.kafka.interval: 60 SECONDS
``` 