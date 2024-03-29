from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
from aws_utils import *
import yaml
import os

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_lst = app_conf["source_list"]
    for src in src_lst:
        src_config = app_conf[src]
        stg_path = "s3a://spark-faisal-spark/staging/" + src
        if src == "SB":
            txnDF = mysql_data_load(spark, app_secret, src_config) \
                .withColumn("ins_dt", current_date())

            txnDF.show(5, False)

            print("Writing data to S3   >>>>>>>>")
            txnDF.write.mode("overwrite").partitionBy("ins_dt").parquet(stg_path)
            print("Writing completed    <<<<<<<<")

        elif src == "OL":
            pem_path = os.path.abspath(current_dir + "/../" + app_secret["sftp_conf"]["pem"])
            file_path = src_config["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv"
            olTxnDF = sftp_data_load(spark, app_secret, file_path, pem_path) \
                .withColumn("ins_dt", current_date())

            olTxnDF.show(5, False)

            print("Writing data to S3   >>>>>>>>")
            olTxnDF.write.mode("overwrite").partitionBy("ins_dt").parquet(stg_path)
            print("Writing completed    <<<<<<<<")

        elif src == "CP":
            s3_file_path = "s3://" + src_config["s3_conf"]["s3_bucket"] + "/KC_Extract_1_20171009.csv"
            campaignsDF = s3_data_load(spark, s3_file_path) \
                .withColumn("ins_dt", current_date())

            campaignsDF.show(5, False)

            print("Writing data to S3   >>>>>>>>")
            campaignsDF.write.mode("overwrite").partitionBy("ins_dt").parquet(stg_path)
            print("Writing completed    <<<<<<<<")

        elif src == "ADDR":
            addressDF = mongodb_data_load(spark, src_config["mongodb_config"]["database"],
                                          src_config["mongodb_config"]["collection"]) \
                .withColumn("ins_dt", current_date())\
                .withColumn("street", col("address.street"))\
                .withColumn("city", col("address.city"))\
                .withColumn("state", col("address.state"))\
                .drop("_id", "address")

            addressDF.show(5, False)

            print("Writing data to S3   >>>>>>>>")
            addressDF.write.mode("overwrite").partitionBy("ins_dt").parquet(stg_path)
            print("Writing completed    <<<<<<<<")

# spark-submit --master yarn --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com.test/source_data_loading.py
