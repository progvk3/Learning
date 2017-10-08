import org.apache.spark.sql.SparkSession

val df = SparkSession.builder().getOrCreate()

val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/ContainsNull.csv")

df.printSchema()

df.show()

df.na.drop.show()

df.na.fill(100).show()

df.na.fill("Missing").show()
df.na.fill("New Name",Array("Name")).show()
df.na.fill("Missing").na.fill(100).show()
