from libreary.config_parser import ConfigParser

parser = ConfigParser("/Users/ben/Desktop/libre-ary/libreary/config")

print(parser.create_config_for_adapter("local1", "local"))