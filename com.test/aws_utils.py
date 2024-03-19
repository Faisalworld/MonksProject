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


def mongodb_data_load(spark, db_name, coll_name, mongodb_conf):
    print("\nReading data from MongoDB using SparkSession.read.format().")
    # MongoDB source
    students_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", db_name) \
        .option("collection", coll_name) \
        .option("uri", mongodb_conf["mongodb_config"]["uri"])\
        .load()

    return students_df


def s3_data_load(spark, file_path):
    print("\nReading data from S3 using SparkSession.read.csv().")
    # S3 source
    campaigns_df = spark \
        .read \
        .csv(file_path)

    return campaigns_df
