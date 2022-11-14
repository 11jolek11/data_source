from data import DataCSV
import os
import time
import configparser
import requests
import paho.mqtt.client as mqtt
from itertools import count


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_publish(client, userdata, mid):
    print("SENT >>")
    # print(">> SENT" + str(client))

class Manager:
    def __init__(self, path_to_config) -> None:
        self.config = configparser.ConfigParser()
        if not os.path.exists(path_to_config):
            raise FileExistsError

        
        self.config.read(path_to_config)
        self.frequency = 1/int(self.config['Data']['frequency'])

        self._data_config = self.config['Data']

        # TODO: add getting chunks of data from DataCSV
        data = DataCSV(self._data_config['source'])

        labels, holder = data.expose()

        dict_len = max(list(holder[labels[0]].keys()))

        temp = []

    
        # for y in count(0):
            # y = y % dict_len
            # y = y % 1000
        for y in range(4000):
            t_str=""
            y = y % dict_len
            for x in labels:
                t_str += str(holder[x][y])
            temp.append(t_str)

        self.temp = iter(temp)




        




        # TODO: add refreshing after a change in config file

        self._mqtt_config = self.config['MQTT']
        self._http_config = self.config['HTTP']

        if self._data_config['channel'].lower() == 'mqtt':
            self.mqtt()
        elif self._data_config['channel'].lower() == 'http':
            self.http()

    def mqtt(self):
        client = mqtt.Client()
        client.on_publish=on_publish
        client.on_connect=on_connect
        client.connect(self._mqtt_config['broker'], int(self._mqtt_config['port']))
        for _ in range(4000):
            client.publish(next(self.temp))
            time.sleep(self.frequency)
        client.disconnect()

    def http(self):
        pload = {'data': next(self.temp)}
        r = requests.post(self._http_config['host'], data = pload)
        
        



# class MQTT:
#     def __init__(self, broker, port, subject) -> None:
#         pass

if __name__ == "__main__":
    p = Manager('/home/andrzej/Studia/iot/list3/datasource/config/test.ini')
