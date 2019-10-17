import mdml_client as mdml

EXPERIMENT_SESSION = 0

def lv_MDML_connect(userdata):
    global EXPERIMENT_SESSION
    for i in range(len(userdata)):
        dat = userdata[i]
        if not isinstance(dat, str):
            userdata[i] = dat.decode('utf-8')
    experiment_id = userdata[0].upper()
    username = userdata[1]
    password = userdata[2]
    host = userdata[3]
    EXPERIMENT_SESSION = mdml.experiment(experiment_id, username, password, host)

def lv_send_config(filepath):
    global EXPERIMENT_SESSION
    if not isinstance(filepath, str):
        filepath = filepath.decode('utf-8')
    EXPERIMENT_SESSION.add_config(filepath)
    EXPERIMENT_SESSION.send_config()

def lv_publish_data(device_id, data):
    global EXPERIMENT_SESSION
    if not isinstance(device_id, str):
        device_id = device_id.decode('utf-8')
    data = [d.decode('utf-8') for d in data]
    data = '\t'.join(data)
    EXPERIMENT_SESSION.publish_data(device_id, data, '\t', True)
    print("hello labview")
    return data

def lv_MDML_disconnect():
    global EXPERIMENT_SESSION
    EXPERIMENT_SESSION.reset()