package etl

import etl.App.res
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import util.RptUtils

object LoactionRpt {
  def main(args: Array[String]): Unit = {
val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
   val docs = spark
     .sparkContext.textFile("zidian.txt")
     .map(_.split("\\s",-1))
     .filter(_.length>=5)
     .map(t=>(t(1),t(4))).collectAsMap()
    val broad = spark.sparkContext.broadcast(docs)
    val df = spark.read.parquet("B.parquent")
val res =df.rdd.map(row=>{
  var appName = row.getAs[String]("appname")
 if(StringUtils.isBlank(appName)){
     appName=broad.value.getOrElse(row.getAs[String]("appid"),"")
 }
  val requestmode =  row.getAs[Int]("requestmode")
  val processnode =  row.getAs[Int]("processnode")
  val iseffetive =  row.getAs[Int]("iseffective")
  val isbilling =  row.getAs[Int]("isbilling")
  val isbid =  row.getAs[Int]("isbid")
  val iswin =  row.getAs[Int]("iswin")
  val adordeerid =  row.getAs[Int]("adorderid")
  val winprice =  row.getAs[Double]("winprice")
  val adpayment =  row.getAs[Double]("adpayment")
  val reqpty = RptUtils.Reqpt(requestmode,processnode)
  val clickPty = RptUtils.clickPt(requestmode:Int,iseffetive:Int)
  val abPty = RptUtils.adPt(iseffetive:Int,isbilling:Int,
    isbid:Int,iswin:Int,adordeerid:Int,winprice:Double
    ,adpayment:Double)
  val alllist = reqpty++clickPty++abPty
  (appName,alllist)
}).reduceByKey((list1,list2)=>{
  val list3 = list1.zip(list2).map(t=>t._1+t._2)
  list3
}).map(t=>t._1+""+t._2)
    res.saveAsTextFile("d.txt")


































    spark.stop()
  }
}
