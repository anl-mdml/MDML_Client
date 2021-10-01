from .MDML_client import *
name = "MDML_Client"
__version__ = "1.2.1"
multipart_schema = {
    "$schema": "http://merf.egs.anl.gov/mdml-multipart-message-schema#",
    "title": "MultipartMessageSchema",
    "description": "Schema for Kafka MDML data messages that are split into multiple messages",
    "type": "object",
    "properties": {
        "time": {
            "description": "Sent timestamp",
            "type": "number"
        },
        "chunk": {
            "description": "File chunk",
            "type": "string"
        },
        "part": {
            "description": "Message part description",
            "type": "string"
        },
        "filename": {
            "description": "Name of the file",
            "type": "string"
        },
        "encoding": {
            "description": "Encoding used",
            "type": "string"
        }
    },
    "required": [ "time", "chunk", "part", "filename", "encoding" ]
}
stop_funcx_schema = {
    "$schema": "http://merf.egs.anl.gov/mdml-example-stop-funcx-schema#",
    "title": "StopFuncx",
    "description": "Schema for the message that will stop a FuncX function",
    "type": "object",
    "properties": {
        "stop": {
            "description": "ID of the data point",
            "type": "boolean"
        }
    },
    "required": [ "stop" ]
}
