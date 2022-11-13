from data import DataCSV
import os
import configparser


class Manager:
    def __init__(self, path_to_config) -> None:
        self.config = configparser.ConfigParser()
        if os.path.exists(path_to_config):
            self.config.read(path_to_config)
        else:
            raise FileExistsError
        
        



# class MQTT:
#     def __init__(self, broker, port, subject) -> None:
#         pass