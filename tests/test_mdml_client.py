import json
import time
import mdml_client as mdml
from random import randrange

LOCAL = False
if LOCAL:
  KAFKA_HOST = "merf.egs.anl.gov"
  KAFKA_PORT = "9092"
  SCHEMA_HOST = "merf.egs.anl.gov"
  SCHEMA_PORT = "8081"
else:
  KAFKA_HOST = "broker"
  KAFKA_PORT = "9092"
  SCHEMA_HOST = "schema-registry"
  SCHEMA_PORT = "8081"

if not LOCAL:
  time.sleep(60)

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
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
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
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )
  msgs = []
  for msg in consumer.consume(overall_timeout=30):
    msgs.append(msg)
  assert len(msgs) == 5

def test_kafka_mdml_producer_schemaless():
  producer = mdml.kafka_mdml_producer_schemaless(
    topic = "mdml-test-schemaless",
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT
  )

  for _ in range(5):
    producer.produce(json.dumps({
      "time": time.time(),
      "int1": randrange(100),
      "int2": randrange(100)
    }))
    time.sleep(1)
    producer.flush()

def test_kafka_mdml_consumer_schemaless():
  consumer = mdml.kafka_mdml_consumer_schemaless(
    topics = ["mdml-test-schemaless"],
    group = "tests",
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT
  )
  msgs = []
  for msg in consumer.consume(overall_timeout=30):
    msgs.append(msg)
  assert len(msgs) == 5

def test_kafka_mdml_consumer_multiple_topics():
  data_schema = mdml.create_schema({
    "time": time.time(),
    "int1": 1,
    "int2": 2
  }, "Test schema", "Schema used for testing the MDML in GitHub Actions")
  producer1 = mdml.kafka_mdml_producer(
    topic = "mdml-test-multiple-1",
    schema = data_schema,
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )
  producer2 = mdml.kafka_mdml_producer(
    topic = "mdml-test-multiple-2",
    schema = data_schema,
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )
  producer3 = mdml.kafka_mdml_producer(
    topic = "mdml-test-multiple-3",
    schema = data_schema,
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )

  for _ in range(5):
    producer1.produce({
      "time": time.time(),
      "int1": randrange(100),
      "int2": randrange(100)
    })
    producer2.produce({
      "time": time.time(),
      "int1": randrange(100),
      "int2": randrange(100)
    })
    producer3.produce({
      "time": time.time(),
      "int1": randrange(100),
      "int2": randrange(100)
    })
    time.sleep(1)
    producer1.flush()
    producer2.flush()
    producer3.flush()

  consumer = mdml.kafka_mdml_consumer(
    topics = ["mdml-test-multiple-1", "mdml-test-multiple-2", "mdml-test-multiple-3"],
    group = "github_actions",
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )
  msgs = []
  for msg in consumer.consume(overall_timeout=30):
    msgs.append(msg)
  assert len(msgs) == 15


def test_chunking_files():
  with open("big_file.txt", "w") as f:
    f.write("A" * 1024 * 1024)
  data_schema = mdml.create_schema({
    "time": time.time(),
    "int1": 1,
    "int2": 2
  }, "Test schema", "Schema used for testing the MDML in GitHub Actions")
  producer = mdml.kafka_mdml_producer(
    topic = "mdml-test-chunking",
    schema = data_schema,
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )
  
  consumer = mdml.kafka_mdml_consumer(
    topics = ["mdml-test-chunking"],
    group = "github_actions",
    kafka_host = KAFKA_HOST,
    kafka_port = KAFKA_PORT,
    schema_host = SCHEMA_HOST,
    schema_port = SCHEMA_PORT
  )

  for part in mdml.chunk_file("big_file.txt", 750000, file_id="chunked_file.txt"):
    print(part)
    producer.produce(part)
  producer.flush()

  for msg in consumer.consume_chunks(overall_timeout=30):
    print(msg)
    with open(msg[1], "r") as f:
      chunked_file = f.read()
      with open("big_file.txt", "r") as f2:
        orig_file = f2.read()
        assert orig_file == chunked_file

if LOCAL:
  test_chunking_files()