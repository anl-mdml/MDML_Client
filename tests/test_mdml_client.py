import time
import mdml_client as mdml
from random import randrange

def test_create_schema():
  data_schema = mdml.create_schema({
    "time": time.time(),
    "int1": 1,
    "int2": 2
  }, "Test schema", "Schema used for testing the MDML in GitHub Actions")
  assert type(data_schema) == dict

def test_kafka_mdml_producer():
  data_schema = mdml.create_schema({
    "time": time.time(),
    "int1": 1,
    "int2": 2
  }, "Test schema", "Schema used for testing the MDML in GitHub Actions")
  producer = mdml.kafka_mdml_producer(
    topic = "mdml-test-github-actions",
    schema = data_schema,
    kafka_host = "broker",
    kafka_port = 9092,
    schema_host = "schema-registry",
    schema_port = 8081
  )

  for _ in range(5):
    producer.produce({
      "time": time.time(),
      "int1": randrange(100),
      "int2": randrange(100)
    })
    time.sleep(1)
    producer.flush()

def test_kafka_mdml_consumer():
  consumer = mdml.kafka_mdml_consumer(
    topics = ["mdml-test-github-actions"],
    group = "github_actions",
    kafka_host = "broker",
    kafka_port = 9092,
    schema_host = "schema-registry",
    schema_port = 8081
  )
  msgs = []
  for msg in consumer.consume(overall_timeout=30):
    msgs.append(msg)
  assert len(msgs) == 5
