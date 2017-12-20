import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()

val wc = sc.textFile("file:///home/vasanth/Learning/Scala/my_first_program.scala").flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

wc.collect()

