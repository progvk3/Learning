import org.apache.spark.sql.SparkSession

val df = SparkSession.builder().getOrCreate()

val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/ContainsNull.csv")

df.printSchema()

df.show()
