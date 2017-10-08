import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

//Use option to get header or ask spark to infer the datatype
val df_t = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/CitiGroup2006_2008")

df_t.head(5)

df_t.printSchema()

df_t.select("Date").show()

df_t


for(n <- df_t.head(5)){
  println(n)
}

val df_t2 = df_t.withColumn("HighPlusLow",df_t("High")+df_t("Low"))

df_t2.show()

df_t2.select(df_t2("HighPlusLow").as("HPL"),df_t2("Date")).show()

import spark.implicits._
df_t2.filter($"Close" > 480).show()

//OR
df_t2.filter("Close > 480").show()

df_t2.filter($"Close" > 480).count()

df_t2.filter($"Close" < 480 && $"High" < 480).show()
