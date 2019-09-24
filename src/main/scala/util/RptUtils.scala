package util

object RptUtils {
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
