import cv2
import time
import pandas as pd
import numpy as np
from base64 import b64encode
from random import randrange

import mdml_client as mdml

# Connect to MDML and login with Globus to use FuncX
exp = mdml.experiment("TEST", "test", "testtest", "merfpoc.egs.anl.gov")
exp.globus_login()

# Adding experiment configuration file
exp.add_config('examples_config.json', 'dlhub_example')
exp.send_config()
 

from keras.datasets import mnist

# Loading data for this example - MNIST data
(x_train, y_train), (x_test, y_test) = mnist.load_data()
x_test = x_test.copy().reshape(10000,28,28,1)



try:
    while True:
        img_ind = randrange(10000)
        _, img = cv2.imencode('.jpg', x_test[img_ind])
        img_b64bytes = b64encode(img)
        img_byte_string = img_b64bytes.decode('utf-8')

        exp.publish_image("MNIST_IMAGES", img_byte_string)

        func_uuid_dlhub = '8a453f62-978d-432e-8525-31faaa124897' #uuid of DLHub model
        func_uuid_cleanup = '8be32fc0-0fbc-487c-b41f-cbb34f4e2715' #formats DLHub model output

        # Starting debugger
        exp.start_debugger()

        dat = x_test[img_ind:img_ind+1].tolist()
        funcx_callback = {
            "endpoint_uuid": "a62a830a-5cd1-42a8-a4a8-a44fa552c899",
            "function_uuid": func_uuid_cleanup,
            "save_intermediate": False
        }
        exp.use_dlhub(dat, "MNIST_DLHUB", func_uuid_dlhub, funcx_callback)

        time.sleep(5)
except KeyboardInterrupt:
    exp.reset(hard_reset=True)
    exp.disconnect()


