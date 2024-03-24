from pyspark.sql import SparkSession
import yaml
import os.path
from pyspark.sql.functions import current_date, col
import aws_utils as ut

if __name__ == '__main__':

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    tgt_list = app_conf['target_list']
    for tgt in tgt_list:
        print('Preparing', tgt, 'data,')
        tgt_conf = app_conf[tgt]

        if tgt == 'REGIS_DIM':
            print('Loading the source data,')
            for src in tgt_conf['source_data']:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_location"] + "/" + src

                src_df = ut.read_parquet_from_s3(spark, file_path)
                src_df.show()
                if src == 'ADDR':
                    src_df = src_df.withColumn("street", col("address.street")) \
                        .withColumn("city", col("address.city")) \
                        .withColumn("state", col("address.state")) \
                        .drop("address")

                src_df.createOrReplaceTempView(src)
                src_df.printSchema()
                src_df.show(5, False)

            print('Preparing the', tgt, 'data,')
            regis_dim_df = spark.sql(tgt_conf['loadingQuery'])
            regis_dim_df.show()
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            ut.write_data_to_redshift(regis_dim_df,
                                      jdbc_url,
                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                      tgt_conf['tableName'])
            print("Completed   <<<<<<<<<")



# spark-submit --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4"\
#   --jars "/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar" \
#   com.test/target_data_loading.py