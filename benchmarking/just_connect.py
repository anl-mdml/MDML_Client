import time
import mdml_client as mdml

exp = mdml.experiment("TEST", "test", "testtest", "merfpoc.egs.anl.gov")

time.sleep(5)

exp.disconnect()