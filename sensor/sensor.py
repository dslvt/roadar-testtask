from datetime import datetime
import requests
import random
import time
import logging

url = "http://controller:8000/sensor/"

log = logging.getLogger(__name__)

message_per_second = 300

print('Sensor started')

while True:
    message = {"datetime": str(datetime.now()), "payload": random.randint(0, 100)}
    x = requests.post(url, json=message)

    time.sleep(1 / message_per_second)



