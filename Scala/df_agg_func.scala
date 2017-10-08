import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df_t = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/Sales.csv")


df.printSchema()
