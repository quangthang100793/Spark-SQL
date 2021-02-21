import org.apache.spark.sql.SparkSession

object Demo8 {
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
    df.show()

    df.write.saveAsTable("employee")
    df.write.format("csv").saveAsTable("employee_csv")

    val employee = spark.sql("SELECT * FROM employee")
    employee.show

    val employee_csv = spark.sql("SELECT * FROM employee_csv")
    employee_csv.show


    spark.close
  }
}
