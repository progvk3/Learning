import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/Netflix_2011_2016.csv")

df.printSchema()
df.count()

df.head(5)

df.describe().show()

//df.withColumn("HV Ratio",df("High")/df("low")).show()

//df.filter($"Date" === df.select(max("High"))).show()

val max_high = df.select(max("High")).collect()

df.filter("High = 716.159996").show()


//df.filter($"Date" === 716.159996).show()


df.filter("Close < 600").select(countDistinct("Date")).show()

df.select(corr("High","Volume"))
