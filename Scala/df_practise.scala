import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

//Use option to get header or ask spark to infer the datatype
val df_t = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/CitiGroup2006_2008")

df_t.head(5)

df_t.printSchema()

df_t.select("Date").show()
