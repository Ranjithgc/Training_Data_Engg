import json
import sys
import random
import requests
import os
from dotenv import load_dotenv
load_dotenv()

def slack_noitfy():
    url = os.getenv("WEBHOOK_URL")
    message = ("Success")
    title = (f"New Incoming Message :zap:")
    slack_data = {
        "username": "Execution Success",
        "icon_emoji": ":satellite:",
        "channel" : "practice",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)