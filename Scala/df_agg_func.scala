import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/Sales.csv")


df.printSchema()


df.groupBy("Company").mean().show()


df.groupBy("Company").count().show()

df.groupBy("Company").min().show()

df.groupBy("Company").sum().show()

df.groupBy("Company","Person").sum().show()
