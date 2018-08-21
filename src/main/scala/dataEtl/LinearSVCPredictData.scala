package dataEtl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import excutor.MainClass
import executor.MainObject.sparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import service.DataTreatingServiceImpl

import scala.collection.mutable.ArrayBuffer

class LinearSVCPredictData {
    def getDataSource() ={
      val sc = sparkSession.sparkContext

      /** **获取基础数据 ***/
      val mainClass = new MainClass()
      import scala.collection.JavaConverters._
      val dataTreatingServiceImpl: DataTreatingServiceImpl = mainClass.getDataTreatingServiceImpl
      val productsPurchasedByUser = dataTreatingServiceImpl.getProductsPurchasedByUser.asScala
      val userPurchasedProductTime = dataTreatingServiceImpl.getUserPurchasedProductTimeInfo.asScala
      val userBuyProductsInfo = dataTreatingServiceImpl.getUserBuyProducts.asScala

      //获取当前月月份与上个月月份
      val date = new Date()
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM")
      val simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd")
      val nowMonth = simpleDateFormat.format(date)
      val nowDate = simpleDateFormat2.format(date)

      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.add(Calendar.MONTH,-1)
      val lastMonth = simpleDateFormat.format(calendar.getTime)

      import sparkSession.implicits._

      /** **基础数据转成RDD且缓存 ***/
      val productsAndUserDF = sc.parallelize(productsPurchasedByUser).map{col=>
        (col.getUserId,col.getProdcutId,nowMonth,lastMonth,nowDate)
      }.toDF("userId","productId","month","frontMonth","nowDate")

      val userPurchasedTimeDF = sc.parallelize(userPurchasedProductTime).map{col=>
        UserPurchasedProductTime(col.getUserId, col.getProductId, col.getPurchasedMoth,        col.getPurchasedStartTime, col.getPurchaseEndTime)
      }.toDF()

      val userBuyProductsInfoDF = sc.parallelize(userBuyProductsInfo).map { info =>
        UserBuyProductsInfo(info.getUserId, info.getProductId, info.getTradeDate, info.getQuantity)
      }.toDF()

      /**********数据结构：用户，产品，月份，上月份******/
      productsAndUserDF.createGlobalTempView("baseData")

      /** ************把用户uuid转换成唯一随机数值 ***************/

      val userUuid2Value = sc.parallelize(dataTreatingServiceImpl.getUserId2UserValue.asScala).map{
        col=> val userId = col.get("userId").toString
          val uservalue = col.get("uservalue").asInstanceOf[Double]
          (userId,uservalue)
      }.toDF("userId","userValue").persist(StorageLevel.MEMORY_AND_DISK)

      /** ************把产品uuid转换成唯一随机数值 **************/

      val productUuid2Value = sc.parallelize(dataTreatingServiceImpl.getProduct2ProductValue.asScala).map{
        col=> val userId = col.get("prodcutId").toString
          val uservalue = col.get("prodcutValue").asInstanceOf[Double]
          (userId,uservalue)
      }.toDF("productId","productValue").persist(StorageLevel.MEMORY_AND_DISK)


      /** ******数据格式：用户，产品，月份，上个月份，上个月交易量 ***********/
      var collectDataFrame = productsAndUserDF.join(userBuyProductsInfoDF,
        productsAndUserDF.col("userId") === userBuyProductsInfoDF.col("userId") &&
          productsAndUserDF.col("productId") === userBuyProductsInfoDF.col("productId") &&
          productsAndUserDF.col("frontMonth") === userBuyProductsInfoDF.col("tradeDate")
        , "left").select(productsAndUserDF.col("userId"), productsAndUserDF.col("productId"),
        productsAndUserDF.col("month"), productsAndUserDF.col("frontMonth"),
        userBuyProductsInfoDF.col("quantity")
      )

      /** **********把DataFramed的销售量为空的替换为0 ***********/
      collectDataFrame = collectDataFrame.na.fill(Map("quantity" -> 0))

      /** *****用户购买产品日期与距离上次购买天数，
        * 如果第一次够买，则上次购买天数为购买时间减去平台上线时间
        */
      /** *******数据格式：用户，产品，交易时间，距离上次交易时间间隔（天） ********/
      userPurchasedTimeDF.createGlobalTempView("purchasedTime")

      val userPurchaseProductInterval = sparkSession.sql(
        "SELECT a1.userId as userIdd, a1.productId as productIdd, a1.month as purchasedMoth, MIN(case when ISNULL(DATEDIFF(a1.nowDate,b1.purchaseEndTime)) = false then DATEDIFF(a1.nowDate,b1.purchaseEndTime) else DATEDIFF(a1.nowDate,'2017-08-01') END ) as purchaseIntervalTime " +
          " FROM global_temp.baseData a1 LEFT JOIN  global_temp.purchasedTime b1 ON a1.userId = b1.userId and a1.productId = b1.productId  and a1.month > b1.purchasedMoth" +
          " group by a1.userId, a1.productId, a1.month"
      )

      import org.apache.spark.sql.functions._
      var mergeDataframe = collectDataFrame.join(userPurchaseProductInterval,
        collectDataFrame.col("userId")===userPurchaseProductInterval.col("userIdd")&&                     collectDataFrame.col("productId")===userPurchaseProductInterval.col("productIdd")&&
        collectDataFrame.col("month")===userPurchaseProductInterval.col("purchasedMoth"),"left"
      ).select(collectDataFrame.col("userId"),collectDataFrame.col("productId"),
        collectDataFrame.col("month"),collectDataFrame.col("quantity"),
        userPurchaseProductInterval.col("purchaseIntervalTime"))

      mergeDataframe = mergeDataframe.join(userUuid2Value, "userId").join(productUuid2Value,  "productId")
        .select(mergeDataframe.col("userId"),mergeDataframe.col("productId"),
          $"userValue", $"productValue", $"month", $"quantity",$"purchaseIntervalTime")

      val predictData = mergeDataframe.rdd.mapPartitions{iter=>
        val list = new ArrayBuffer[Vector]
        while(iter.hasNext){
           val row = iter.next()
          val userValue = row.get(2).asInstanceOf[Double]
          val productValue = row.get(3).asInstanceOf[Double]
          val quantity = row.get(5).asInstanceOf[Double]
          val purchaseIntervalTime = row.get(6).asInstanceOf[Integer].toDouble
          val vector = Vectors.dense(Array(userValue,productValue,quantity,purchaseIntervalTime))
          list += vector
        }
        list.toIterator
      }

      (predictData,userUuid2Value,productUuid2Value)
    }
}

object LinearSVCPredictData{
  def apply: LinearSVCPredictData = new LinearSVCPredictData()
}