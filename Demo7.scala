import org.apache.spark.sql.SparkSession

object Demo7 {
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

    val peopleDF = spark.read.format("json").load("D:\\Data1\\resources\\people.json")
    peopleDF.show
    peopleDF.select("name", "age").write.format("csv").save("namesAndAges.csv")

    val people_age = spark.read.format("csv").load("namesAndAges.csv").toDF("name","age")
    people_age.show

    spark.close
  }
}
