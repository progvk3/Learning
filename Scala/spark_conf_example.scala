val conf = new SparkConf().setAppName("vasanth_learning").setMaster("yarn-client")
val sc = SparkContext(conf)

val lines = sc.textFile("/user/testing/my_first_program.scala")
println(lines.count)
