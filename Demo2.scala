import org.apache.spark.sql.SparkSession

object Demo2 {
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
      .appName("Creating DataFrame From CSV files")
      .getOrCreate()
    val sc = spark.sparkContext
    val SQLContext = spark.sqlContext

    import spark.implicits._

    val df = spark
      .read
      .option("inferSchema", "true")
      .option("header","true")
      .option("delimiter", ",")
      .csv("D:\\Data1\\emp.csvh")
    df.show()
    df.printSchema()

    spark.close
  }
}
