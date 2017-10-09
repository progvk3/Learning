import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()

val wc = sc.textFile("file:///home/vasanth/Learning/Scala/my_first_program.scala").Map(line => line.split(" ")).flatMap(map => (map,1)).reduceByKey(_ + _)

wc.show()
