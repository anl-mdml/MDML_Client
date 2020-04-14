#!/bin/bash

python3.6  overload_nodered_data.py --host=52.4.135.44 --send_images=True --test_throughput=True --messages=1000 &&
python3.6  overload_nodered_data.py --host=52.4.135.44 --send_images=True --messages=1000 &&
python3.6  overload_nodered_data.py --host=52.4.135.44 --test_throughput=True --messages=1000 &&
python3.6  overload_nodered_data.py --host=52.4.135.44 --messages=1000
