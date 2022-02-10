import argparse

def main(args):
    import time
    import mdml_client as mdml
    # args.schemaHost defaults to args.host 
    if args.schemaHost is None:
        args.schemaHost = args.host

    data_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-test-producer-data-schema#",
        "title": "Producer test",
        "description": "Schema for testing the MQTT connector",
        "type": "object",
        "properties": {
            "time": {
                "description": "Unix time the data point",
                "type": "number"
            },
            "msg_id": {
                "description": "Interger message ID - increments by 1",
                "type": "number"
            },
            "int1": {
                "description": "number of CPU cores",
                "type": "number"
            },
            "int2": {
                "description": "average percent usage of CPU",
                "type": "number"
            }
        },
        "required": [ "time", "msg_id", "int1", "int2" ]
    }

    producer = mdml.kafka_mdml_producer(
        topic = args.topic,
        schema = data_schema,
        kafka_host = args.host,
        kafka_port = args.port,
        schema_host = args.schemaHost,
        schema_port = args.schemaPort
    )

    try:
        for i in range(args.num_msgs):
            producer.produce({
                "time": time.time(),
                "msg_id": i,
                "int1": 1,
                "int2": 2
            })
            producer.flush()
            print(f"sent: {i}")
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print("KeyboardInterrupt: stopping early")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A generic MDML producer for example purposes")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic to consume")
    parser.add_argument('-s', dest="host", required=True,
                        help="Hostname of the kafka broker")
    parser.add_argument('-n', dest="num_msgs", default=3,
                        help="Number of messages to send [default: 3]")
    parser.add_argument('-port', dest="port", default=9092,
                        help="Kafka broker port number [default: 9092]")
    parser.add_argument('--schema-host', dest="schemaHost", default=None,
                        help="Schema registry host [defaults to the -s HOST parameter")
    parser.add_argument('--schema-port', dest="schemaPort", default=8081,
                        help="Schema registry port [default: 8081]")
    parser.add_argument('--sleep', dest="sleep", default=1,
                        help="Seconds to sleep after each message [default: 1]")
    args = parser.parse_args()
    main(args)
