import argparse

def main(args):
    import time
    import mdml_client as mdml
    print(args)
    data_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-test-producer-data-schema#",
        "title": "Example Experiment data",
        "description": "Schema for testing the MQTT connector",
        "type": "object",
        "properties": {
            "time": {
                "description": "Unix time the data point",
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
        "required": [ "time", "int1", "int2" ]
    }

    # Create a producer for the experiment
    producer = mdml.kafka_mdml_producer(
        topic = args.topic,
        schema = data_schema,
        kafka_host = args.host,
        kafka_port = args.port,
        schema_host = args.schemaHost,
        schema_port = args.schemaPort
    )

    # Start experiment
    input("Press enter to start the experiment...\n")
    mdml.start_experiment(
        id = args.id_experiment, 
        topics = [args.topic], 
        producer_kwargs = {
            "kafka_host": args.host,
            "kafka_port": args.port,
            "schema_host": args.schemaHost,
            "schema_port": args.schemaPort
        }
    )

    # Sending experiment data
    input("Press enter to start streaming data...\n")
    try:
        for i in range(100):
            producer.produce({
                "time": time.time(),
                "int1": i,
                "int2": i
            })
            producer.flush()
            print(f"Data sent: {i}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("done")

    # Stopping experiment
    input("Press enter to stop the experiment...\n")
    mdml.stop_experiment(
        id = args.id_experiment, 
        producer_kwargs = {
            "kafka_host": args.host,
            "kafka_port": args.port,
            "schema_host": args.schemaHost,
            "schema_port": args.schemaPort
        }
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Admin tool to delete a Kafka Connector")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic to consume")
    parser.add_argument('-i', dest="id_experiment", required=True,
                        help="Experiment ID")
    parser.add_argument('-s', dest="host", required=True,
                        help="Hostname of the kafka broker")
    parser.add_argument('-port', dest="port", default=9092,
                        help="Kafka broker port number [default: 9092]")
    parser.add_argument('--schema-host', dest="schemaHost", default=None,
                        help="Schema registry host [defaults to -s HOST parameter]")
    parser.add_argument('--schema-port', dest="schemaPort", default=8081,
                        help="Schema registry port [default: 8081]")
    args = parser.parse_args()
    # args.schemaHost defaults to args.host
    if args.schemaHost is None:
        args.schemaHost = args.host
    main(args)