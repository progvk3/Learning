import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/Netflix_2011_2016.csv")

df.printSchema()

df.head(5)

df.describe()

//df.withColumn("HV Ratio",)
