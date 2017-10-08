import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header",true).option("inferSchema",true).csv("file:///home/vasanth/Learning/Scala/Spark DataFrames/CitiGroup2006_2008")

//df.head(6)


for(row <- df.head(5)){
  println(row)
}
