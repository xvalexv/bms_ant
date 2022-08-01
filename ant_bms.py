#!/usr/bin/env python3
import socket
import bluetooth
from binascii import unhexlify
import time
import codecs
import logging
import struct
from paho.mqtt import client as mqtt_client
import random
import json
import serial
import configparser
import threading
from influxdb import InfluxDBClient
from time import gmtime, strftime

def set_logger():
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(processName)s %(threadName)s %(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    _LOGGER.addHandler(ch)
    return _LOGGER



class DATA_LOGGER:
  def __init__(self, host, port, username, password, topic, dbname, send_data, send_mathod):
    self.host = host
    self.port = port
    self.username = username
    self.password = password
    self.topic = topic
    self.send_data = send_data
    self.send_mathod = send_mathod

    def connect_mqtt() -> mqtt_client:

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                _LOGGER.info("Connected to MQTT Broker!" + str(userdata) + str(flags))
            else:
                _LOGGER.error("Failed to connect, return code %d\n", rc)
                return None
        try:
            client_id = f'python-mqtt-{random.randint(0, 100)}'
            _LOGGER.info("Trying mqtt connect host: {}  port: {} username: {}  client_id: {}".format(self.host,self.port,self.username,client_id))
            self.client = mqtt_client.Client(client_id)
            self.client.username_pw_set(self.username, self.password)
            self.client.on_connect = on_connect
            self.client.connect (self.host, self.port)
            #self.client.loop_start()

        except Exception as ex:
            _LOGGER.error("connect mqtt error: " + ex)
            return None
        #return client
    def connect_influx():
        self.client = InfluxDBClient(host, port, username, password, dbname)

    if self.send_mathod == 'MQTT':
        connect_mqtt()
    elif self.send_mathod == 'INFLUX':
        connect_influx()


  def send_battery_data(self, battery_data, table_name):
      if self.send_mathod == 'MQTT':
        if self.send_data:
          _LOGGER.info("Trying mqtt publish topic: {}  ".format(self.topic))
          battery_data['name'] = table_name

          json_battery_data = json.dumps(battery_data, indent=4)
          #_LOGGER.debug(json_battery_data)

          try:
              self.client.publish(self.topic + "/" + "battery_data", json_battery_data)
              for key in battery_data:
                  #print(key + "/" + key  str(battery_data[key]))
                  self.client.publish(self.topic + "/" + key, str(battery_data[key]))
          except Exception as ex:
              _LOGGER.error("mqtt publish error: " + str(ex))
              #exit(0)
          _LOGGER.info("mqtt publish OK!")
      elif self.send_mathod == 'INFLUX':
          _LOGGER.info("Influxdb publish to table : {}  ".format(table_name))
          try:
              _LOGGER.debug("Fixing types")
              for key in ['port', 'cell_count', 'discharge_status', 'charge_status', 'balance_status', 'soc', 'power',
                          'mosfet_temp', 'balance_temp', 'sensor_temp_1', 'sensor_temp_2', 'sensor_temp_3',
                          'sensor_temp_4']:
                  if battery_data[key] is None:
                      battery_data[key] = int(0)
                  else:
                      battery_data[key] = int(battery_data[key])
              for key in ['remain_ah', 'bms_current', 'bms_v', 'cell_avg', 'cell_min', 'cell_max', 'power']:
                  if battery_data[key] is None:
                      battery_data[key] = float(0)
                  else:
                      battery_data[key] = float(battery_data[key])
              for key in range(8):
                  key = "cell_amps_" + str(key + 1)
                  if battery_data[key] is None:
                      battery_data[key] = float(0)
                  else:
                      battery_data[key] = float(battery_data[key])


              influx_row = {}
              influx_row['measurement'] = table_name
              influx_row['tags'] = {}
              influx_row['time'] = strftime("%m-%d-%Y %H:%M:%S", gmtime())
              influx_row['fields'] = battery_data
              influx_points = []
              influx_points.append(influx_row)
          except Exception as ex:
            _LOGGER.error("Influxdb fix fields prepare data error: " + str(ex))

          try:
            self.client.write_points(influx_points)
          except Exception as ex:
            _LOGGER.error("Influxdb publish error: " + str(ex))
            #exit(0)
          _LOGGER.info("Influxdb publish OK!")

          #print(battery_data)


class ANT_BMS:
    def __init__(self):
        def read_config():
            config = configparser.ConfigParser()
            config.read("ant_bms.conf")
            return config


        config = read_config()
        self.connect_retry_count = config.getint("BLUETOOTH","connect_retry_count")
        self.connect_retry_delay = config.getint("BLUETOOTH","connect_retry_delay")
        self.need_discover = config.getboolean("BLUETOOTH","need_discover")

        with open('batteries.json', 'r') as batteries_file:
            batteries_data = json.load(batteries_file)

        self.batteries = batteries_data
        self.need_send_data = config.getboolean("LOGGER","need_send_logger")
        if self.need_send_data:

            logger_method = config.get("LOGGER", "logger_method")
            if logger_method == 'MQTT':
                self.mqtt = DATA_LOGGER(config.get("MQTT","logger_host"), config.getint("MQTT","logger_port"), config.get("MQTT","logger_user"),
                                        config.get("MQTT","logger_password"), config.get("MQTT","logger_topic"),"", self.need_send_data, logger_method)
            elif logger_method == 'INFLUX':
                self.mqtt = DATA_LOGGER(config.get("INFLUX","logger_host"), config.getint("INFLUX","logger_port"), config.get("INFLUX","logger_user"),
                                        config.get("INFLUX","logger_password"), "", config.get("INFLUX","logger_database"), self.need_send_data, logger_method)

    def discover_bluetooth(self):
        def discover_devices():
            _LOGGER.info("Discovering bluetooth devices")
            nearby_devices = bluetooth.discover_devices(lookup_names=True, flush_cache=True, duration=30)
            devices_count = len(nearby_devices)
            bms_devices_count = 0
            # _LOGGER.info("found {} devices ".format(devices_count))
            for addr, name in nearby_devices:
                _LOGGER.info("-------------------------- {} - {} --------------------------".format(addr, name))
                if name[0:7] == "BMS-ANT":
                    _LOGGER.info("============================== Found BMS-ANT device! Name: {} Addres: {} ==============================".format(name, addr))
                    bms_devices_count += 1
                    # batteries.append({'addr': addr, 'table_name': name})
                s = bluetooth.find_service(address=addr)
                for services in s:
                    _LOGGER.info(" Protocol: {}, Port: {}, host: {}".format(services["protocol"], services["port"], services["host"]))
            _LOGGER.info("found total: {} bluetooth devices and {} BMS-ANT devices  ".format(devices_count, bms_devices_count))
            return bms_devices_count
        if self.need_discover:
            try:
                while discover_devices() == 0:
                    _LOGGER.debug("No BMS-ANT devices found. Sleeping 10 sec")
                    time.sleep(30)
            except (KeyboardInterrupt, SystemExit):
                _LOGGER.debug("interrupted!")
                exit(0)
    def run(self):
        def start_reading_thread(self,battery,idx):
            def read_battery_data(self,battery):
                def decode_data(response_data, cell_count):
                    if len(response_data) == 280:
                        try:
                            _LOGGER.debug("decode_data: " + str(response_data))
                            # remain ah
                            battery['remain_ah'] = int(response_data[79 * 2:82 * 2 + 2], 16) / 1000000

                            battery['discharge_status'] = int(response_data[104 * 2:104 * 2 + 2])
                            battery['charge_status'] = int(response_data[103 * 2:103 * 2 + 2])
                            battery['balance_status'] = int(response_data[105 * 2:105 * 2 + 2])

                            try:
                                balance_data = response_data[134 * 2:135 * 2 + 2]
                            except:
                                balance_data = 0xFFFF

                            balance_data_unpack = struct.unpack('>H', unhexlify(balance_data))[0]

                            '''for i in range(cell_count):
                                battery['bal_st' + str(i)] = str(balance_data_unpack >> i & 1)'''

                            # SoC (1)

                            battery['soc'] = int(response_data[(74 * 2):(75 * 2)], 16)

                            # Power (2)
                            data = (response_data[(111 * 2):(114 * 2 + 2)])
                            _LOGGER.debug("power raw " + str(int(data, 16)))
                            if int(data, 16) > 2147483648:
                                battery['power'] = int(-(2 * 2147483648) + int(data, 16))
                            else:
                                battery['power'] = int(data, 16)

                            # BMS current (3)
                            data = (response_data[(70 * 2):(73 * 2 + 2)])
                            _LOGGER.debug("bms_current raw " + str(int(data, 16)))
                            if int(data, 16) > 2147483648:
                                battery['bms_current'] = float((-(2 * 2147483648) + int(data, 16)) / 10)
                            else:
                                battery['bms_current'] = float(int(data, 16) / 10)

                            # BMS V (4)
                            data = response_data[8:12]
                            data = struct.unpack('>H', unhexlify(data))[0] * 0.1
                            battery['bms_v'] = float(data + 0.7)
                            # 0.7 was added as BMS low.

                            # Cell_avg (5)
                            data = (response_data[(121 * 2):(122 * 2 + 2)])
                            battery['cell_avg'] = float(struct.unpack('>H', unhexlify(data))[0] / 1000)

                            # Cell_min (6)
                            data = (response_data[(119 * 2):(120 * 2 + 2)])
                            battery['cell_min'] = float(struct.unpack('>H', unhexlify(data))[0] / 1000)

                            # Cell_max (7)
                            data = (response_data[(116 * 2):(117 * 2 + 2)])
                            battery['cell_max'] = float(struct.unpack('>H', unhexlify(data))[0] / 1000)

                            for i in range(cell_count):
                                data = response_data[((6 + i * 2) * 2):((7 + i * 2) * 2 + 2)]
                                battery['cell_amps_' + str(i + 1)] = float(struct.unpack('>H', unhexlify(data))[0] / 1000)

                            data_power_temp = (response_data[92 * 2:92 * 2 + 2])
                            battery['mosfet_temp'] = int(data_power_temp, 16)

                            data_balance_temp = (response_data[94 * 2:94 * 2 + 2])
                            battery['balance_temp'] = int(data_balance_temp, 16)

                            data_cell_temp_1 = (response_data[96 * 2:96 * 2 + 2])
                            battery['sensor_temp_1'] = int(data_cell_temp_1, 16)

                            data_cell_temp_2 = (response_data[98 * 2:98 * 2 + 2])
                            battery['sensor_temp_2'] = int(data_cell_temp_2, 16)

                            data_cell_temp_3 = (response_data[100 * 2:100 * 2 + 2])
                            battery['sensor_temp_3'] = int(data_cell_temp_3, 16)

                            data_cell_temp_4 = (response_data[102 * 2:102 * 2 + 2])
                            battery['sensor_temp_4'] = int(data_cell_temp_4, 16)

                            json_battery_data = json.dumps(battery, indent=4)
                            _LOGGER.debug(json_battery_data)

                            return battery

                        except Exception as ex:
                            _LOGGER.error("Decode_data error: " + str(ex))
                            return None
                    else:
                        _LOGGER.error("Error response length: " + str(len(response_data)) + " must be 280")

                def read_bluetooth(bluetooth_battery_addr, bluetooth_battery_port):
                    _LOGGER.debug("Rading data from bloutooth")

                    def ant_connect_socket(serverMACAddress, port, retry_count):
                        _LOGGER.info("Trying to connect address: {}  port: {} retry count: {}".format(serverMACAddress, port, retry_count))
                        try:
                            s = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
                        except Exception as ex:
                            _LOGGER.error("Socket error: " + ex)
                            return None

                        try:
                            try:
                                s.connect((serverMACAddress, port))
                            except (KeyboardInterrupt, SystemExit):
                                s.close()
                                _LOGGER.debug("interrupted!")
                                exit(0)
                            _LOGGER.info("Connected!")
                            connected = True
                        except Exception as ex:
                            connected = False
                            s.close()
                            _LOGGER.error("Connect error:" + str(ex))

                        if not connected:
                            retry_count += 1
                            _LOGGER.error("Connect error, retrying: " + str(retry_count))
                            if retry_count <= self.connect_retry_count:
                                time.sleep(self.connect_retry_delay)
                                _LOGGER.debug("Waiting {} sec: ".format(self.connect_retry_delay))
                                ant_connect_socket(serverMACAddress, port, retry_count)
                            else:
                                _LOGGER.error("Connect retry {} > {} exiting".format(retry_count, self.connect_retry_count))
                                return None
                        else:
                            return s

                    def read_and_decode_ant_answer(bluetooth_socket):
                        try:
                            test_word = 'DBDB00000000'
                            _LOGGER.debug("Sending test word: " + str(test_word))
                            bluetooth_socket.send(codecs.decode(test_word, 'hex'))
                            time.sleep(3)
                            data = bluetooth_socket.recv(140)
                            _LOGGER.debug("Got result: " + str(data))
                        except Exception as ex:
                            _LOGGER.error("Data send/receive error: " + str(ex))
                            return None
                        _LOGGER.debug("Closing bloutooth socket")
                        bluetooth_socket.close()
                        if len(data) > 0:
                            return codecs.encode(data, 'hex')
                        else:
                            return None

                    try:
                        ant_s = ant_connect_socket(bluetooth_battery_addr, bluetooth_battery_port, 0)
                        if ant_s is not None:
                            response_data = read_and_decode_ant_answer(ant_s)
                            return response_data
                        else:
                            _LOGGER.error("Response error")
                            return None
                    except Exception as ex:
                        _LOGGER.error("Connect and read data error: " + str(ex))
                        return None

                def read_rs485(rs485_port, rs485_baudrate):

                    rs485_serial = serial.Serial(port=rs485_port, baudrate=rs485_baudrate, parity=serial.PARITY_EVEN, stopbits=serial.STOPBITS_ONE, bytesize=serial.SEVENBITS, timeout=1)

                    try:
                        test_word = 'DBDB00000000'
                        _LOGGER.debug("Sending test word to rs485: " + str(test_word))
                        rs485_serial.write(codecs.decode(test_word, 'hex'))
                        time.sleep(3)
                        data = rs485_serial.read(170)
                        _LOGGER.debug("Got result: " + str(data))
                    except Exception as ex:
                        _LOGGER.error("Data send/receive rs485 error: " + str(ex))
                        return None
                    _LOGGER.debug("Closing serial")
                    rs485_serial.close()
                    if len(data) > 0:
                        _LOGGER.debug("Response data" + str(codecs.encode(data, 'hex')))
                        return codecs.encode(data, 'hex')
                    else:
                        return None

                connect_type = battery['connect_type']
                cell_count = battery['cell_count']
                table_name = battery['table_name']

                if connect_type == 'bluetooth':
                    bluetooth_battery_addr = battery['addr']
                    bluetooth_battery_port = battery['port']
                    _LOGGER.info("Getting data from battery bluetooth addr: {} port: {}  to MQTT table {} ".format(bluetooth_battery_addr, bluetooth_battery_port, table_name))
                    response_data = read_bluetooth(bluetooth_battery_addr, bluetooth_battery_port)

                elif connect_type == 'rs485':
                    rs485_battery_port = battery['port']
                    rs485_battery_baudrate = battery['baudrate']
                    _LOGGER.info("Getting data from battery rs485 port: {} baudrate: {} MQTT table {} ".format(rs485_battery_port, rs485_battery_baudrate, table_name))
                    response_data = read_rs485(rs485_battery_port, rs485_battery_baudrate)

                else:
                    response_data = None

                if response_data is not None:
                    battery_data = decode_data(response_data, cell_count)
                    if self.need_send_data:
                        self.mqtt.send_battery_data(battery_data, table_name)
            try:
                _LOGGER.info("Starting reading thread idx:" +str(idx)+ " for battery: " + json.dumps(battery, indent=4))
                while True:
                    read_battery_data(self,battery)
                    _LOGGER.debug("Sleeping 30 sec")
                    time.sleep(30)
            except (KeyboardInterrupt, SystemExit):
                _LOGGER.debug("interrupted!")
                exit(0)
        _LOGGER.info("Starting battery reading threads ... " + json.dumps(self.batteries, indent=4))
        for battery in self.batteries:
            idx = list(self.batteries).index(battery)
            self.run_event = threading.Event()
            self.run_event.set()
            my_thread = threading.Thread(target=start_reading_thread, args=(self,battery, idx))
            my_thread.start()

_LOGGER = set_logger()
ant = ANT_BMS()
if __name__ == '__main__':
    ant.run()