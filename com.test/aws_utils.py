def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)
    # url = "jdbc:redshift://myredshiftcluster.590183684400.eu-west-1.redshift-serverless.amazonaws.com:5439/dev?user=admin&password=Admin1234"


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)


def mysql_data_load(spark, app_secret, src_config):
    jdbc_params = {"url": get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": src_config["mysql_conf"]["query"],
                   "numPartitions": "2",
                   "partitionColumn": src_config["mysql_conf"]["partition_column"],
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }

    print("\nReading data from MySQL DB using SparkSession.read.format().")
    # MYSQL source
    txn_df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()

    return txn_df


def sftp_data_load(spark, app_secret, file_path, pem_file_path):
    print("\nReading data from SFTP server using SparkSession.read.format().")
    # SFTP source
    ol_txn_df = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", app_secret["sftp_conf"]["hostname"]) \
        .option("port", app_secret["sftp_conf"]["port"]) \
        .option("username", app_secret["sftp_conf"]["username"]) \
        .option("pem", pem_file_path) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(file_path)

    return ol_txn_df


def mongodb_data_load(spark, db_name, coll_name):
    print("\nReading data from MongoDB using SparkSession.read.format().")
    # MongoDB source
    students_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", db_name) \
        .option("collection", coll_name) \
        .load()
    # .option("uri", "mongodb://ec2-54-155-250-235.eu-west-1.compute.amazonaws.com:27017")\

    return students_df


def s3_data_load(spark, file_path):
    print("\nReading data from S3 using SparkSession.read.csv().")
    # S3 source
    campaigns_df = spark \
        .read \
        .option("header", True) \
        .option("delimiter", "|") \
        .csv(file_path)

    return campaigns_df


def read_parquet_from_s3(spark, file_path):
    return spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .parquet(file_path)


def write_data_to_redshift(txn_df, jdbc_url, s3_path, redshift_table_name):
    txn_df.coalesce(1).write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("tempdir", s3_path) \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", redshift_table_name) \
        .mode("overwrite") \
        .save()
