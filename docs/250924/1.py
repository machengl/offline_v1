from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveExternalTableExample") \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")

query_all_db = "show databases;"

spark.sql(query_all_db).show()

spark.stop()

