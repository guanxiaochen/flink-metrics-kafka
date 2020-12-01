## flink-metrics-kafka

## 配置
| 配置项| 默认值| 必填| 例| 说明 |
| --- | --- | ---| ---| --- |
| bootstrap.servers| | 是 | localhost:9092| kafka地址 |
| cluster| | 否 | testa | 集群名称 |
| topic| flink-metrics | 否 | | kafka主题 |
| kdyBy| | 否 | host | kafka key属性名 |

## 其他kafka.prop配置(非必填)
| 配置项| 例|
| --- | ---|
| acks| all | 
| retries| 0 | 
| batch.size| 16384 | 
| linger.ms| 1 | 
| buffer.memory| 33554432 |


## 例
```
metrics.reporters: kafka
metrics.reporter.kafka.class: org.apache.flink.metrics.kafka.KafkaReporter
metrics.reporter.kafka.bootstrap.servers: localhost:9092
metrics.reporter.kafka.cluster: cluster4
metrics.reporter.kafka.topic: flink-metrics
metrics.reporter.kafka.keyBy: task_attempt_id
metrics.reporter.kafka.prop.acks: all
metrics.reporter.kafka.prop.retries: 0
metrics.reporter.kafka.prop.batch.size: 16384
metrics.reporter.kafka.prop.linger.ms: 1
metrics.reporter.kafka.prop.buffer.memory: 33554432
metrics.reporter.kafka.prop.interval: 60 SECONDS
``` 