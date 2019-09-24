/*
package Fuxi

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import util.String2Type


//1.
object xiangmu{
  val stuctType=StructType(
    Seq(
      StructField("sessionid", StringType),
      StructField("advertisersid", IntegerType),
      StructField("adorderid", IntegerType),
      StructField("adcreativeid", IntegerType),
      StructField("adplatformproviderid", IntegerType),
      StructField("sdkversion", StringType),
      StructField("adplatformkey", StringType),
      StructField("putinmodeltype", IntegerType),
      StructField("requestmode", IntegerType),
      StructField("adprice", DoubleType),
      StructField("adppprice", DoubleType),
      StructField("requestdate", StringType),
      StructField("ip", StringType),
      StructField("appid", StringType),
      StructField("appname", StringType),
      StructField("uuid", StringType),
      StructField("device", StringType),
      StructField("client", IntegerType),
      StructField("osversion", StringType),
      StructField("density", StringType),
      StructField("pw", IntegerType),
      StructField("ph", IntegerType),
      StructField("long", StringType),
      StructField("lat", StringType),
      StructField("provincename", StringType),
      StructField("cityname", StringType),
      StructField("ispid", IntegerType),
      StructField("ispname", StringType),
      StructField("networkmannerid", IntegerType),
      StructField("networkmannername", StringType),
      StructField("iseffective", IntegerType),
      StructField("isbilling", IntegerType),
      StructField("adspacetype", IntegerType),
      StructField("adspacetypename", StringType),
      StructField("devicetype", IntegerType),
      StructField("processnode", IntegerType),
      StructField("apptype", IntegerType),
      StructField("district", StringType),
      StructField("paymode", IntegerType),
      StructField("isbid", IntegerType),
      StructField("bidprice", DoubleType),
      StructField("winprice", DoubleType),
      StructField("iswin", IntegerType),
      StructField("cur", StringType),
      StructField("rate", DoubleType),
      StructField("cnywinprice", DoubleType),
      StructField("imei", StringType),
      StructField("mac", StringType),
      StructField("idfa", StringType),
      StructField("openudid", StringType),
      StructField("androidid", StringType),
      StructField("rtbprovince", StringType),
      StructField("rtbcity", StringType),
      StructField("rtbdistrict", StringType),
      StructField("rtbstreet", StringType),
      StructField("storeurl", StringType),
      StructField("realip", StringType),
      StructField("isqualityapp", IntegerType),
      StructField("bidfloor", DoubleType),
      StructField("aw", IntegerType),
      StructField("ah", IntegerType),
      StructField("imeimd5", StringType),
      StructField("macmd5", StringType),
      StructField("idfamd5", StringType),
      StructField("openudidmd5", StringType),
      StructField("androididmd5", StringType),
      StructField("imeisha1", StringType),
      StructField("macsha1", StringType),
      StructField("idfasha1", StringType),
      StructField("openudidsha1", StringType),
      StructField("androididsha1", StringType),
      StructField("uuidunknow", StringType),
      StructField("userid", StringType),
      StructField("iptype", IntegerType),
      StructField("initbidprice", DoubleType),
      StructField("adpayment", DoubleType),
      StructField("agentrate", DoubleType),
      StructField("lomarkrate", DoubleType),
      StructField("adxrate", DoubleType),
      StructField("title", StringType),
      StructField("keywords", StringType),
      StructField("tagid", StringType),
      StructField("callbackdate", StringType),
      StructField("channelid", StringType),
      StructField("mediatype", IntegerType)
    )
  )
}
object FuXi {
  def main(args: Array[String]): Unit = {

val spark= SparkSession.builder()
      .appName("")
      .master("local")
  .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val line = spark.sparkContext.textFile("A.log")

     import  spark.implicits._
     val rowRDD = line.map(t=>t.split(",",-1)).filter(t=>t.length>=85).map(arr=>{
   Row(
       arr(0),
       String2Type.toInt(arr(1)),
       String2Type.toInt(arr(2)),
       String2Type.toInt(arr(3)),
       String2Type.toInt(arr(4)),
       arr(5),
       arr(6),
       String2Type.toInt(arr(7)),
       String2Type.toInt(arr(8)),
       String2Type.toDouble(arr(9)),
       String2Type.toDouble(arr(10)),
       arr(11),
       arr(12),
       arr(13),
       arr(14),
       arr(15),
       arr(16),
       String2Type.toInt(arr(17)),
       arr(18),
       arr(19),
       String2Type.toInt(arr(20)),
       String2Type.toInt(arr(21)),
       arr(22),
       arr(23),
       arr(24),
       arr(25),
       String2Type.toInt(arr(26)),
       arr(27),
       String2Type.toInt(arr(28)),
       arr(29),
       String2Type.toInt(arr(30)),
       String2Type.toInt(arr(31)),
       String2Type.toInt(arr(32)),
       arr(33),
       String2Type.toInt(arr(34)),
       String2Type.toInt(arr(35)),
       String2Type.toInt(arr(36)),
       arr(37),
       String2Type.toInt(arr(38)),
       String2Type.toInt(arr(39)),
       String2Type.toDouble(arr(40)),
       String2Type.toDouble(arr(41)),
       String2Type.toInt(arr(42)),
       arr(43),
       String2Type.toDouble(arr(44)),
       String2Type.toDouble(arr(45)),
       arr(46),
       arr(47),
       arr(48),
       arr(49),
       arr(50),
       arr(51),
       arr(52),
       arr(53),
       arr(54),
       arr(55),
       arr(56),
       String2Type.toInt(arr(57)),
       String2Type.toDouble(arr(58)),
       String2Type.toInt(arr(59)),
       String2Type.toInt(arr(60)),
       arr(61),
       arr(62),
       arr(63),
       arr(64),
       arr(65),
       arr(66),
       arr(67),
       arr(68),
       arr(69),
       arr(70),
       arr(71),
       arr(72),
       String2Type.toInt(arr(73)),
       String2Type.toDouble(arr(74)),
       String2Type.toDouble(arr(75)),
       String2Type.toDouble(arr(76)),
       String2Type.toDouble(arr(77)),
       String2Type.toDouble(arr(78)),
       arr(79),
       arr(80),
       arr(81),
       arr(82),
       arr(83),
       String2Type.toInt(arr(84))
     )
    })
val df = spark.createDataFrame(rowRDD,xiangmu.stuctType)
     df.write.parquet("res\\fx.parquent")

  val read =  spark.read.parquet("res\\fx.parquent")
    read.createOrReplaceTempView("table")
    val ressql =spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    val conf = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",conf.getString("user"))
    pro.getProperty("password",conf.getString("password"))

    ressql.write.mode(SaveMode.Append).jdbc(conf.getString("jdbc"),conf.getString("table"),pro)

val   zhibiao= spark.read.parquet("B.parquent")
    val zidian = spark.sparkContext.textFile("").map(row=>{
    val arr =     row.split("\\s").filter(_.length>=5)
      (arr(1),arr(4))
    }).collectAsMap()
   val broad = spark.sparkContext.broadcast(zidian)

val reszhibiao = zhibiao.map(row=>{
  var appname = row.getAs[String]("appname")
  if(StringUtils.isBlank(appname)){
   appname= broad.value.getOrElse(row.getAs[String]("appid"),"")
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
  val list = utilfuxu.adPt(iseffetive:Int,isbilling:Int,
    isbid:Int,iswin:Int,adordeerid:Int,winprice:Double
    ,adpayment:Double)
  val list2  =utilfuxu.clickPt(requestmode,iseffetive)
  val list3  =utilfuxu.Reqpt(requestmode,processnode)
  (appname,list++list2++list3)
}).rdd
  reszhibiao.saveAsTextFile("d.txt")




val tag= spark.read.parquet("B.parquent")
val restag= tag.map(row=>{
  val str = utilbiaoqian.biaoqian(row)
  val tag = biaoqian1.matag(row)
  (str,tag)
}).show()























    spark.stop()
  }
}
object utilfuxu{
  def Reqpt(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode==1 && processnode ==1){
      List[Double](1,0,0)
    }else if(requestmode==1 && processnode ==2){
      List[Double](1,1,0)
    }else if(requestmode==1 && processnode ==3){
      List[Double](1,1,1)
    }else{
      List(0,0,0)
    }
  }
  def clickPt(requestmode:Int,iseffetive:Int):List[Double]={
    if(requestmode==2 && iseffetive==1){
      List[Double](1,0)
    }else if(requestmode==3 && iseffetive==1){
      List[Double](0,1)
    }else{
      List(0,0)
    }
  }
  def adPt(iseffetive:Int,isbilling:Int,
           isbid:Int,iswin:Int,adordeerid:Int,winprice:Double
           ,adpayment:Double):List[Double]={
    if(iseffetive==1 && isbilling==1 && isbid==1){
      if(iseffetive==1 && isbilling==1 && iswin==1 &&adordeerid!=0){
        List[Double](1,1,winprice/1000,adpayment/1000)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List(0,0,0,0)
    }
  }
}
object utilbiaoqian{
  def biaoqian(args:Row): String ={
    args match{
      case t if StringUtils.isNotBlank(t.getAs[String]("imei")) => "IM"+t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac")) => "MC"+t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) => "ID"+t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid")) => "OD"+t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) => "AD"+t.getAs[String]("androidid")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) => "IM"+t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) => "MC"+t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) => "ID"+t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5")) => "OD"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) => "AD"+t.getAs[String]("androididmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) => "IM"+t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) => "MC"+t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) => "ID"+t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1")) => "OD"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) => "AD"+t.getAs[String]("androididsha1")
      case _ => "其他"
    }
  }
}
object biaoqian1{
  def matag(row:Row):List[(String,Int)]={
    var list = List[(String,Int)]()
 val adType  = row.getAs[Int]("adspacetype")
  adType match {
    case x => list:+=("LC"+x,1)
    case x<9=>list:+=("LC0"+x,1)
  }
    val adname  = row.getAs[Int]("adspacetypename")
    list:+= ("LN"+adname,1)

    list
  }
}*/
