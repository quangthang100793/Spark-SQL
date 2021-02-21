import org.apache.spark.sql.SparkSession


object Demo6 {
  def main(args: Array[String]): Unit = {
    //Turn off Spark Warning/Inforation Messages
    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)
    import org.apache.log4j._
    // tuong tu import.* trong java, chu y dau _
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // val fileName = "d:/spark-demo/data/plays.csv"
    // co the viet "d:\\soark-demo/.... vi \ trong scala la ki tu dac biet
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Creating DataSet From CSV files")
      .getOrCreate()
    val sc = spark.sparkContext
    val SQLContext = spark.sqlContext

    import spark.implicits._
    // phải để dưới biến tạo spark

    val df = spark.read.json("D:\\Data1\\employee.json")

    df.createOrReplaceTempView("employee")
    df.show()

    val result1 = spark.sql("SELECT sum(salary) SumSalary, min(salary) MinSalary, max(salary) MaxSalary, avg(salary) AvgSalary, count(*) CountEmp FROM employee")
  // có thể dùng var result var result
    result1.show()

    val result2 = spark.sql("SELECT gender, sum(salary) SumSalary FROM employee GROUP BY gender")
    // có thể dùng var result; khúc này chỉ cần viết result mà không cần var
    result2.show()

    spark.close
  }

}
