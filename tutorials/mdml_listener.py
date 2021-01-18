# Parameters to run the example
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--id", help="MDML experiment ID",
                        required=True)
parser.add_argument("--host", help="MDML instance host",
                        required=True)
parser.add_argument("--username", help="MDML username",
                        required=True)
parser.add_argument("--password", help="MDML password",
                        required=True)
args = parser.parse_args()

import time
import mdml_client as mdml

exp = mdml.experiment(args.id, args.username, args.password, args.host)

time.sleep(3)
try:
    def callback_func(client, userdata, message): # Only message is used here
        print(f'TOPIC: {message.topic}\nPAYLOAD: {message.payload.decode("utf-8")}')
    exp.listener(callback = callback_func)
except KeyboardInterrupt:
    print("Quitting...")
finally:
    exp.disconnect()
