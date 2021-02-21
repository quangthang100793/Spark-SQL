import org.apache.spark.sql.SparkSession

object Demo9 {
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
      .master("local[2]")  // 2 core tương ứng với 2 partitions, vì partitions được tạo nên từ số core
      .appName("Creating DataSet From CSV files")
      .getOrCreate()
    val sc = spark.sparkContext
    val SQLContext = spark.sqlContext

    import spark.implicits._
    // phải để dưới biến tạo spark

    val df = spark.read.json("D:\\Data1\\customer.json")
    df.show

    df.write
      .format("csv")
      .partitionBy("country","state_province","city")
      .bucketBy(3,"customer_id")
      .sortBy("customer_id")
      .saveAsTable("customer")


    spark.close
  }
}
