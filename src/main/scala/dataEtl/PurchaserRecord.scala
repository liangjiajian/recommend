package dataEtl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

/*********购买者消费记录数据处理***********/
class PurchaserRecord {

  /*
   用户购买产品信息RDD（格式：用户,产品）转化成（格式：用户，产品，月份,上月份）
   月份最小值为当前系统版本上线时间，月份最大值为当前月的上个月
   */

  def  productsPurchasedByUserRddTransform(sourceData:RDD[Tuple2[String,String]],startTime:String)={
    /*******获取当前日期（月）******/
    val  simpleDateFormat = new SimpleDateFormat("yyyy-MM")
    /***获取当前月份**/
    val nowDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()))
    val calendar = Calendar.getInstance()
    calendar.setTime(nowDate)
    /****获取系统上线时间***/
    val systemStartTime = simpleDateFormat.parse(startTime)
    val systemStartTimeCalendar = Calendar.getInstance()
    systemStartTimeCalendar.setTime(systemStartTime)
    /******往用户购物记录里加入时间列*********/
    val sourceDataAddColumn = sourceData.map{line=>
      val list = ArrayBuffer[Tuple4[String,String,String,String]]()
      /*****只记录系统上线时间到上个月之间的月份*****/
      val calendar2 = calendar.clone().asInstanceOf[Calendar]
      calendar2.add(Calendar.MONTH,-1)
      val startTimeYear = systemStartTimeCalendar.get(Calendar.YEAR)
      val startTimeMonth = systemStartTimeCalendar.get(Calendar.MONTH)
      var editTimeYear = calendar2.get(Calendar.YEAR)
      var editTimeMonth = calendar2.get(Calendar.MONTH)

      while(editTimeYear>=startTimeYear){
        if(editTimeYear > startTimeYear){
          val date = simpleDateFormat.format(calendar2.getTime)
          calendar2.add(Calendar.MONTH,-1)
          val frontDate = simpleDateFormat.format(calendar2.getTime)
          val info = Tuple4(line._1,line._2,date,frontDate)
          editTimeYear = calendar2.get(Calendar.YEAR)
          editTimeMonth = calendar2.get(Calendar.MONTH)
          list += info
        }else if(editTimeYear == startTimeYear && editTimeMonth >= startTimeMonth){
          val date = simpleDateFormat.format(calendar2.getTime)
          calendar2.add(Calendar.MONTH,-1)
          val frontDate = simpleDateFormat.format(calendar2.getTime)
          val info = Tuple4(line._1,line._2,date,frontDate)
          editTimeYear = calendar2.get(Calendar.YEAR)
          editTimeMonth = calendar2.get(Calendar.MONTH)
          list += info
        }else{
          calendar2.add(Calendar.YEAR,-1)
          editTimeYear = calendar2.get(Calendar.YEAR)
        }
      }
      list
    }.flatMap(col=>col.map(l=>Row(l._1,l._2,l._3,l._4)))
    sourceDataAddColumn
  }

}

object PurchaserRecord{

  def apply: PurchaserRecord = new PurchaserRecord()



}
