# wis2box-subscriber-demo

Demo code to subscribe to global cache and send data to S3-compatible storage end-point

Uppdate data_mapping.csv with topics to subscriber and the folders on wis2box-incoming the data should be sent to

define .env with info on credentials for your broker

.. code-block:: bash

    MQTT_HOST=broker.host.address
    MQTT_USERNAME=thebrokerusername
    MQTT_PWD=thebrokerpassword
    MQTT_PORT=1883
    STORAGE_USERNAME=wis2box
    STORAGE_PWD=Wh00data!
    STORAGE_URL=http://s3-host/bucket-name
    LOG_LEVEL=INFO

Build wis2box-subscriber container:

.. code-block:: bash

    docker build -t wis2box-subscriber .

Run wis2box-subscriber container, ensure data_mapping is available at /tmp/data_mapping.csv on container

.. code-block:: bash
    
    docker run --env-file .env --rm -it wis2box-subscriber