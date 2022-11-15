from data import DataCSV
import os
import time
import configparser
import requests
import paho.mqtt.client as mqtt
from itertools import count
import argparse
import json

avaible_datasets = {
    1: "./config/biomechanical.ini",
    2: "./config/Book_Dataset_1.ini",
    3: "./config/delhie_weather_dataset.ini",
    4: "./config/sea_level.ini",
    5: "./config/wine_reviews.ini",
}


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_publish(client, userdata, mid):
    print("SENT >>")
    # print(">> SENT" + str(client))

class Manager:
    def __init__(self, path_to_config=None, id=1) -> None:
        self.config = configparser.ConfigParser()
        # if not os.path.exists(path_to_config):
        #     raise FileNotFoundError


        
        if path_to_config is None:
            path_to_config = avaible_datasets[id]
        self.config.read(path_to_config)
        self._data_config = self.config['Data']
        
        
        self.frequency = 1/float(self._data_config['frequency'])

        

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
                t_str += str(holder[x][y]) + "  "
            temp.append(t_str)

        self.temp = iter(temp)




        




        # TODO: add refreshing after a change in config file

        self._mqtt_config = self.config['MQTT']
        self._http_config = self.config['HTTP']

        if self._data_config['channel'].lower() == 'mqtt':
            self.mqtt()
        elif self._data_config['channel'].lower() == 'http':
            # self.test()
            self.http()

    def mqtt(self):
        client = mqtt.Client()
        client.on_publish=on_publish
        client.on_connect=on_connect
        client.connect(self._mqtt_config['broker'], int(self._mqtt_config['port']))
        for _ in range(4000):
            client.publish(self._mqtt_config['topic'], json.dumps({'data': next(self.temp)}))
            time.sleep(self.frequency)
        client.disconnect()

    def http(self):
        for _ in range(4000):
            pload = {'data': next(self.temp)}
            time.sleep(self.frequency)
            r = requests.post(self._http_config['host'] +":"+ self._http_config['port'], data = pload)

    # def test(self):
    #     for _ in range(300):
    #         print(next(self.temp))
        
        



# class MQTT:
#     def __init__(self, broker, port, subject) -> None:
#         pass

if __name__ == "__main__":
    pass
    p = Manager("./config/wine_reviews.ini")
#     # parser = argparse.ArgumentParser()

