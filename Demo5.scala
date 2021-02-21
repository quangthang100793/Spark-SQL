import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Sales(year: Int, id: Int, name: String, sales: Double)

object Demo5 {
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

    val schema = new StructType()
      .add("year",IntegerType,true)
      .add("id",IntegerType,true)
      .add("name",StringType,true)
      .add("sales",DoubleType,true)



    val df = spark.read
      .option("delimiter","\t")
      .schema(schema)
      .csv("D:\\Data1\\sales.txt") // phải gõ \\ vì chỉ gõ \ đây là kí tự đặc biệt nên đường dẫn phải gõ \\

    val ds = df.as[Sales]
    ds.show

    spark.close
  }
}
