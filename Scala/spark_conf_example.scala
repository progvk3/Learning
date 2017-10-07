//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf

//val conf = new SparkConf().setAppName("vasanth_learning").setMaster("yarn-client")
//val sc = new SparkContext(conf)
sc.setLogLevel("WARN")
val lines = sc.textFile("/user/testing/my_first_program.scala")
println(lines.count)
println(lines.first())
