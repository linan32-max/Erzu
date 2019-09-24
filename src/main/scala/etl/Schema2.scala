package etl

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.xml.Properties

object Schema2 {
  def main(args: Array[String]): Unit = {
//    val Array(inputpath)= args
   val sp =  SparkSession.builder()
      .appName("")
      .master("local")
     .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
val df = sp.read.parquet("B.parquent")
    df.createOrReplaceTempView("log")
  val res = sp.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
  //res.write.partitionBy("provincename","cityname").json("c.json")
  val load = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("password",load.getString("jdbc.password"))
res.write
 .mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3360/spark",
  "procity",pro)

  sp.stop()

  }
}
