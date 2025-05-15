# kafka-current-assignment

トピックの割り当てを`partition reassignment tool`の`reassignment-json-file`の形式で出力する。

## 使い方

```sh
./kafka-current-assignment --bootstrap-server localhost:9092 --topic-list test
```

出力例

```json
{
  "version": 1,
  "partitions": [
    {
      "topic": "test",
      "partition": 0,
      "replicas": [
        0,
        1
      ],
      "log_dirs": [
        "/tmp/kafka-logs",
        "/tmp/kafka-logs"
      ]
    }
  ]
}
```

## build

```sh
./gradlew build
```
