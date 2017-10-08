import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.csv("/Spark DataFrames/CitiGroup2006_2008")

df.head(5)
