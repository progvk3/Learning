class student{
  var company:String = "Corecompete"
  def employee(string:String)={
    println(s"Hi $string")
  }
}

object hello{
  def main(args:Array[String]){
    var s = new student
    s.employee("Vasanth")
  }
}
