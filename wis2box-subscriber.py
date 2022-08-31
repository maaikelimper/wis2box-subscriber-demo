import os
import requests
import json
import ssl
import logging
import random
import paho.mqtt.client as mqtt

def sub_connect(client, userdata, flags, rc, properties=None):
    logging.info(f"on connection to subscribe: {mqtt.connack_string(rc)}")
    for s in ["cache/v04/#"]:
        client.subscribe(s, qos=1)

def sub_on_message(client, userdata, msg):
    """
      do something with the message
    """
    print("topic: ",msg.topic)
    print("payload: ",msg.payload.decode('utf-8'))

MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PWD = os.getenv("MQTT_PWD")
MQTT_HOST = os.getenv("MQTT_HOST")
LOG_LEVEL = os.getenv("LOG_LEVEL","DEBUG")
logging.basicConfig(level=LOG_LEVEL)
logging.getLogger("mqtt").setLevel(logging.INFO)

def run_wis2box_subscriber():
    r = random.Random()
    client_id = f"{__name__}_{r.randint(1,1000):04d}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
    client.tls_set(certfile=None,keyfile=None,cert_reqs=ssl.CERT_REQUIRED)
    client.username_pw_set(MQTT_USERNAME, MQTT_PWD)
    client.on_connect = sub_connect
    client.on_message = sub_on_message
    client.connect(MQTT_HOST)
    client.loop_forever()

def main():
    logging.info("run wis2box-subscriber")
    logging.info(f'MQTT_HOST={MQTT_HOST}')
    logging.info(f'MQTT_USERNAME={MQTT_USERNAME}')
    logging.info(f'MQTT_PASSWORD={MQTT_PWD}')
    run_wis2box_subscriber()

if __name__ == "__main__":
    main()