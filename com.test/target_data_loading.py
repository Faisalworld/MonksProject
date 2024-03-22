from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import yaml

if __name__ == "__main__":

    spark = SparkSession.builder\
        .appName("Reading orgransation data")\
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_conf_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret = yaml.load(app_secret_path, Loader=yaml.FullLoader)





