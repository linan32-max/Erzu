package Testweek

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object  week{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val line = sc.textFile("json.txt")
    val result = collection.mutable.ListBuffer[String]()
   val res =  line.map(row =>{
      val json = JSON.parseObject(row)
      val status = json.getIntValue("status")
      if(status==1){
        val  json1 = json.getJSONObject("regeocode")
        val pois = json1.getJSONArray("pois")
        for(item<-pois.toArray){
          val json = item.asInstanceOf[JSONObject]
          result.append(json.getString("type"))
        }
      }
     result.mkString(";")
    })
   val res1 =  res.flatMap(t=>{
      t.split(";").map((_,1))
    }).reduceByKey(_+_)
    println(res1.collect().toBuffer)
  }
}
object test01 {
  def main(args: Array[String]): Unit= {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val jsonstr =sc.textFile("json.txt")
    val jsonparse= jsonstr.map(t => {
      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      val jsonparse = JSON.parseObject(t)
      // 判断状态是否成功
      val status = jsonparse.getIntValue("status")
      if (status == 1) {
        // 接下来解析内部json串，判断每个key的value都不能为空
        val regeocodeJson = jsonparse.getJSONObject("regeocode")
        if (regeocodeJson != null && !regeocodeJson.keySet().isEmpty) {
          //获取pois数组
          val poisArray = regeocodeJson.getJSONArray("pois")
          if (poisArray != null && !poisArray.isEmpty) {
            // 循环输出
            for (item <- poisArray.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val json = item.asInstanceOf[JSONObject]
                buffer.append(json.getString("businessarea"))
              }
            }
          }
        }
      }
      buffer.mkString(",")
    })


    jsonparse.flatMap(line=>{
      line.split(",").map((_,1))
    }).reduceByKey(_+_).foreach(println)



  }
}
object Test1_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val jsonstr = sc.textFile("json.txt")
    val jsonbuf = jsonstr.map(t=>{
      val buf = collection.mutable.ListBuffer[String]()
      val parsejson = JSON.parseObject(t)
      val status: Int = parsejson.getIntValue("status")
      if(status == 1){
        val regeocodejson = parsejson.getJSONObject("regeocode")
        if(regeocodejson != null && !regeocodejson.keySet().isEmpty){
          val pois: JSONArray = regeocodejson.getJSONArray("pois")
          if(pois != null && !pois.isEmpty){
            for(item <- pois.toArray){
              if(item.isInstanceOf[JSONObject]){
                val json = item.asInstanceOf[JSONObject]
                buf.append(json.getString("type"))
              }
            }
          }
        }
      }
      buf.mkString(";")
    })
    val res = jsonbuf.flatMap(t => {
      t.split(";").map((_, 1))
    }).reduceByKey(_ + _)

    res.foreach(println)
    sc.stop()
  }
}
