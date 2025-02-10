#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""P1 meter device for solaredge_meterproxy

   This file is indended to be used with https://github.com/nmakel/solaredge_meterproxy by nmakel
   and is to be stored in the devices folder

   It consumes MQTT messages with meterdata from the P1 meter created with https://github.com/marcelrv/p1-reader

"""

from __future__ import division
from collections import deque

import logging
import sys
import time
import json
import paho.mqtt.client as mqtt
from datetime import datetime

__author__ = ["Jacques Mulders"]
__version__ = "1.0"
__copyright__ = "Copyright 2022, Marcel Verpaalen"
__license__ = "GPL"
__credits__ = ["NMakel", "Marcel Verpaalen"]

class MovingAverage(object):
    def __init__(self, size):
        """
        Initialize your data structure here.
        :type size: int
        """
        self.queue = deque(maxlen=size)

    def next(self, val):
        """
        :type val: int
        :rtype: float
        """
        self.queue.append(val)
        return sum(self.queue) / len(self.queue)


lastValues = {}
logger = logging.getLogger()

def on_connect(client, userdata, flags, rc):
    logger.info(
        f"MQTT connected to {userdata['host']}:{userdata['port']} - topic: '{userdata['meterValuesTopic']}' with result code {rc}.")
    client.subscribe(userdata["meterValuesTopic"])
    if userdata['willTopic'] is not None:
        client.publish(userdata['willTopic'], "MeterProxy Connected " +
                       str(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


def on_message(client, userdata, message):
    global lastValues
#   logger.debug("Dump variable %s " %  json.dumps( userdata, indent=4, sort_keys=True))
    decoded_message = str(message.payload.decode("utf-8"))
    lastValues = json.loads(decoded_message)
   print(lastValues['powerImportedActual'], type(lastValues['powerImportedActual']))
   print(float(lastValues['powerImportedActual']), type(float(lastValues['powerImportedActual'])))
   print(lastValues['powerExportedActual'], type(lastValues['powerExportedActual']))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.info(F"Unexpected MQTT disconnection, with result code {rc}.")


def device(config):

    # Configuration parameters:
    #
    # host              ip or hostname of MQTT server
    # port              port of MQTT server
    # keepalive         keepalive time in seconds for MQTT server
    # meterValuesTopic  MQTT topic to subscribe to to receive meter values

    host = config.get("host", fallback="localhost")
    port = config.getint("port", fallback=1883)
    keepalive = config.getint("keepalive", fallback=60)
    meterValuesTopic = config.get("meterValuesTopic", fallback="meter")
    willTopic = config.get("willTopic", fallback=None)
    willMsg = config.get("willMsg", fallback="MeterProxy Disconnected")

    topics = {
        "host": host,
        "port": port,
        "meterValuesTopic": meterValuesTopic,
        "willTopic": willTopic
    }

    try:
        client = mqtt.Client(userdata=topics)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        if willTopic is not None:
            client.will_set(willTopic, payload=willMsg, qos=0, retain=False)
        client.connect(host, port, keepalive)
        client.loop_start()
        logger.debug(
            f"Started MQTT connection to server - topic: {host}:{port}  - {meterValuesTopic}")
    except:
        logger.critical(
            f"MQTT connection failed: {host}:{port} - {meterValuesTopic}")

    return {
        "client": client,
        "host": host,
        "port": port,
        "keepalive": keepalive,
        "meterValuesTopic": meterValuesTopic,
        "willTopic": willTopic,
        "willMsg": willMsg
    }


def values(device):
    if not device:
        return {}
    global lastValues
    submitValues = {}

    submitValues['l1n_voltage'] = float(lastValues['instantaneousVoltageL1'])
    submitValues['l2n_voltage'] = float(lastValues['instantaneousVoltageL2'])
    submitValues['l3n_voltage'] = float(lastValues['instantaneousVoltageL3'])
    submitValues['voltage_ln']  = float(lastValues['instantaneousVoltageL1'])
    submitValues['frequency'] = 50

    submitValues ['power_active'] = (float(lastValues['powerImportedActual']) * 1000) - (float(lastValues['powerExportedActual'])* 1000)
    submitValues ['l1_power_active']= (float(lastValues['instantaneousActivePowerL1Plus']) * 1000) - (float(lastValues['instantaneousActivePowerL1Min']) * 1000)
    submitValues ['l2_power_active']= (float(lastValues['instantaneousActivePowerL2Plus']) * 1000) - (float(lastValues['instantaneousActivePowerL2Min']) * 1000)
    submitValues ['l3_power_active']= (float(lastValues['instantaneousActivePowerL3Plus']) * 1000) - (float(lastValues['instantaneousActivePowerL3Min']) * 1000)
    #calculate current as P1 provided current is rounded to integers   
    submitValues['l1_current'] = abs ( submitValues ['l1_power_active'] ) / float(lastValues['instantaneousVoltageL1'])
    submitValues['l2_current'] = abs ( submitValues ['l2_power_active'] ) / float(lastValues['instantaneousVoltageL2'])
    submitValues['l3_current'] = abs ( submitValues ['l3_power_active'] ) / float(lastValues['instantaneousVoltageL3'])

    submitValues['import_energy_active'] = float(lastValues['electricityImportedT1']) + float(lastValues['electricityImportedT2'])
    submitValues['export_energy_active'] = float(lastValues['electricityExportedT1']) + float(lastValues['electricityExportedT2'])
    submitValues['energy_active'] =  submitValues['import_energy_active'] - submitValues['export_energy_active'] 
    submitValues["_input"] = lastValues

    logger.debug("Dump values %s " %  json.dumps( submitValues, indent=4, sort_keys=True))

    return submitValues
