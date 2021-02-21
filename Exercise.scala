
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Exercise {
  def main(args: Array[String]): Unit = {
    //val date = "2017-01-03"
    val date = args(0)

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
      .master("local[2]") // 2 core tương ứng với 2 partitions, vì partitions được tạo nên từ số core
      .appName("Creating DataSet From CSV files")
      .getOrCreate()
    val sc = spark.sparkContext
    val SQLContext = spark.sqlContext
    // phải để dưới biến tạo spark
    import spark.implicits._

    val schema = new StructType()
      .add("TICKER", StringType, true) // (ten cot, kieu du lieu cua cot, cho phep null hay khong)
      .add("DATE", DateType, true)
      .add("OPEN", DoubleType, true)
      .add("HIGH", DoubleType, true)
      .add("LOW", DoubleType, true)
      .add("CLOSE", DoubleType, true)
      .add("VOLUME", LongType, true)

    val df = spark
      .read
      .option("header", "true")
      .option("dateFormat", "MM/dd/yyyy")
      .schema(schema)
      .csv("D:\\Data1\\VNIndex")
    //df.show
    //df.printSchema()

    //df.filter(s"CLOSE>OPEN and DATE='${date}'").show() // viet dang chuoi thi khong can dau $

    //df.filter($"CLOSE" > $"OPEN" && $"DATE" === lit("2017-01-03")).show() // trong spark sql thi so sanh bang la 3 dau bang ===
    //Liệt kê những cổ phiếu tăng trong một ngày cho trước (tham số).
    // lit la ham tao ra mot cot, chua gia tri hang la 2017-01-03
    //df.filter($"CLOSE" > $"OPEN" && $"DATE".eq(lit("2017-01-03"))).show()

    //df.filter($"CLOSE" > $"OPEN" && $"DATE" === lit(date)).show() // trong spark sql thi so sanh bang la 3 dau bang ===

    //df.agg(max(($"CLOSE"-$"OPEN")/$"OPEN") as "MaxRate").show // agg la mot function de tinh min max count

    // Xác định cổ phiếu tăng giá nhiều nhất (theo tỷ lệ phần trăm) trong một ngày cho trước.

    //Cach 1:

    //    val df1 = df.withColumn("Rate",($"CLOSE"-$"OPEN")/$"OPEN").cache()
    //    df1.show()
    //
    //    val maxRate = df1
    //      .filter(s"DATE='${date}'")
    //      .agg(max("Rate") as "MaxRate")
    //      .take(1)(0)(0)
    //
    //        df1.filter(s"date='${date}' and rate=${maxRate}")
    //            .show()

    //    // take(1)(0)(0), thi (1) la lay ra Array[Row], con (0) lay ra Row, (0) tiep theo lay la
    //
    //    // hoac viet print(maxRate(0).(0)
    //    print(maxRate)
    //
    //    df.filter(s"DATE='${date}' and ((close-open)/open) = ${maxRate}")
    //        .show()
    // Cach 2:

//    val df1 = df.withColumn("Rate", ($"CLOSE" - $"OPEN") / $"OPEN").cache()
//
//
//    val df2 = df1
//      .filter(s"DATE='${date}'")
//      .agg(max("Rate") as "MaxRate")
//      .cache()
//
//    df1.crossJoin(df2)
//      .filter(s"date='${date}' and rate=MaxRate")
//      .show()

    //Lưu dataframe vào file hose1 theo yêu cầu:


//     df.withColumn("year",year($"date"))
//      .withColumn("month",month($"date"))
//      .withColumn("day",dayofmonth($"date"))
//      .write
//      .format("csv")
//      .partitionBy("year","month","day")
//      .bucketBy(2, "ticker")
//      .sortBy("ticker")
//      .saveAsTable("hose1")

    // Luu dataframe vao file hose2 theo yeu cau

  /*  df.filter("ticker ='AAA' or ticker = 'BVH'")
    .withColumn("year",year($"date"))
      .withColumn("month",month($"date"))
      .withColumn("day",dayofmonth($"date"))
      .write
      .format("csv")
      .partitionBy("year","month","ticker")
      .bucketBy(2, "day")
      .sortBy("day")
      .saveAsTable("hose2")*/

    // khong the bucketBy boi cac cot da duoc partition by,

    //Luu dataframe vao file hose3

    df.filter("ticker ='AAA' or ticker = 'BVH'")
      .withColumn("year",year($"date"))
      .withColumn("month",month($"date"))
      .withColumn("day",dayofmonth($"date"))
      .write
      .format("json")
      .partitionBy("year","month","ticker")
      .bucketBy(2, "day")
      .sortBy("day")
      .saveAsTable("hose3")

    //df1.filter(s"date='${date}' and rate=${maxRate}")
    //.show()

    spark.close()
  }
}
