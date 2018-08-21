package dataEtl

import java.text.SimpleDateFormat
import java.util.Date

import excutor.MainClass
import executor.MainObject.sparkSession
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vectors,Vector}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import service.DataTreatingServiceImpl

import scala.collection.mutable.ArrayBuffer

/**********暂停使用********/
class EtlPurchaseRecord {

  /**************获取建模数据***************/
  def getSourceDataFrame(whereCondition:String)={
  val mergeDataframe = etlData(whereCondition)
//    mergeDataframe.repartition(1).write.csv("D:\\sparkMlModel\\test2")
  val  sources = mergeDataframe.rdd.mapPartitions { ite =>
    val list = new ArrayBuffer[LabeledPoint]()
    while (ite.hasNext) {
      val row = ite.next
      val label = row.get(0).asInstanceOf[Double]
      val userValue = row.get(3).asInstanceOf[Double]
      val productValue = row.get(4).asInstanceOf[Double]
//      val productActivity = row.get(6).asInstanceOf[Double]
//      val joinActivity = row.get(7).asInstanceOf[Double]
      val quantity = row.get(6).asInstanceOf[Double]
      val purchaseIntervalTime = row.get(7).asInstanceOf[Integer].toDouble
      val features = Vectors.dense(Array(userValue, productValue,quantity,purchaseIntervalTime))
      list += LabeledPoint(label, features)
    }
    list.toIterator
  }
    sources
 }

  /***************获取需要预测的数据****************/
  def  getPredictData(whereCondition:String)={
    val mergeDataframe = etlData(whereCondition)
    val  sources = mergeDataframe.rdd.mapPartitions { ite =>
      val list = new ArrayBuffer[Vector]()
      while (ite.hasNext) {
        val row = ite.next
        val userValue = row.get(3).asInstanceOf[Double]
        val productValue = row.get(4).asInstanceOf[Double]
        val productActivity = row.get(6).asInstanceOf[Double]
        val joinActivity = row.get(7).asInstanceOf[Double]
        val quantity = row.get(8).asInstanceOf[Double]
        val features = Vectors.dense(Array(userValue, productValue, productActivity, joinActivity, quantity))
        list += features
      }
      list.toIterator
    }
    sources
  }



  /************数据处理************/
  def etlData(whereCondition:String) ={
    val sc = sparkSession.sparkContext

    /** **获取基础数据 ***/
    val mainClass = new MainClass()
    import scala.collection.JavaConverters._
    val dataTreatingServiceImpl: DataTreatingServiceImpl = mainClass.getDataTreatingServiceImpl
    val productsActivity = dataTreatingServiceImpl.getProductsActivity.asScala
    val productsPurchasedByUser = dataTreatingServiceImpl.getProductsPurchasedByUser.asScala
    val userBuyProductsInfo = dataTreatingServiceImpl.getUserBuyProducts.asScala
    val userJoinActivity = dataTreatingServiceImpl.getUserJoinActivity.asScala
    val userPurchasedProductTime = dataTreatingServiceImpl.getUserPurchasedProductTimeInfo.asScala
    /** *系统开始时间 **/
    val startTime = dataTreatingServiceImpl.getSystemStartTime

    /** **基础数据转成RDD且缓存 ***/
    val productsActivityRdd = sc.parallelize(productsActivity).map { ac =>
      ProductsActivity(ac.getProductId, ac.getActivityStartDate, ac.getActivityEndDate)
    }

    val productsPurchasedByUserRdd = sc.parallelize(productsPurchasedByUser).map { info =>
      (info.getUserId, info.getProdcutId)
    }

    val userBuyProductsInfoRdd = sc.parallelize(userBuyProductsInfo).map { info =>
      UserBuyProductsInfo(info.getUserId, info.getProductId, info.getTradeDate, info.getQuantity)
    }

    val userJoinActivityRdd = sc.parallelize(userJoinActivity).map { info =>
      UserJoinActivity(info.getUserId, info.getProductId, info.getPromotionsId, info.getDateOfPurchase)
    }

    val userPurchasedProductTimeRdd = sc.parallelize(userPurchasedProductTime).map { info =>
      UserPurchasedProductTime(info.getUserId, info.getProductId, info.getPurchasedMoth, info.getPurchasedStartTime, info.getPurchaseEndTime)
    }

    /** ***用户产品全额日期转换***（用户，产品）=》(用户，产品，月份，上月份)  添加上月份列方便获取产品上个月销售量 */
    val productsPurchasedByUserRddTransformRdd = PurchaserRecord.apply.productsPurchasedByUserRddTransform(productsPurchasedByUserRdd, startTime)
    val productsPurchasedByUserRddTransformString = "userId,productId,month,frontMonth"
    var schema = StructType(productsPurchasedByUserRddTransformString.split(",").map { lin =>
      new StructField(lin, StringType, true)
    })
    val productsPurchasedByUserRddTransformDataFrame = sparkSession.createDataFrame(productsPurchasedByUserRddTransformRdd, schema).cache()

    /**********数据结构：用户，产品，月份，上月份******/
    productsPurchasedByUserRddTransformDataFrame.createGlobalTempView("baseData")

    import sparkSession.implicits._
    val productsActivityDataFrame = productsActivityRdd.toDF()
    val userBuyProductsInfoDataFrame = userBuyProductsInfoRdd.toDF()
    val userJoinActivityDataFrame = userJoinActivityRdd.toDF()
    val userPurchasedProductTimeDataFrame = userPurchasedProductTimeRdd.toDF()

    import org.apache.spark.sql.functions._

    /** ************把用户uuid转换成唯一随机数值 ***************/
//    var userUuid2Value = productsPurchasedByUserRddTransformDataFrame.select($"userId").distinct().withColumn("userValue", monotonically_increasing_id * 1.0)


  var userUuid2Value = productsPurchasedByUserRddTransformDataFrame
    .select($"userId")
    .distinct().orderBy($"userId")
    .rdd.map(col=>col.get(0))
    .zipWithIndex().map(col=>(col._1.toString,col._2.toDouble))
    .toDF("userId","userValue")

    /** ************把产品uuid转换成唯一随机数值 **************/
//    var productUuid2Value = productsPurchasedByUserRddTransformDataFrame.select($"productId").distinct().withColumn("productValue", monotonically_increasing_id * 10.0)

    var productUuid2Value = productsPurchasedByUserRddTransformDataFrame
      .select($"productId")
      .distinct().orderBy($"productId")
      .rdd.map(col=>col.get(0))
      .zipWithIndex().map(col=>(col._1.toString,col._2.toDouble))
      .toDF("productId","productValue")


    /** *********月份包括用户购买与没有购买该产品的月份 ********/
    /** ******数据格式：用户，产品，月份，上个月份，上个月交易量 ***********/
    var collectDataFrame = productsPurchasedByUserRddTransformDataFrame.join(userBuyProductsInfoDataFrame,
      productsPurchasedByUserRddTransformDataFrame.col("userId") === userBuyProductsInfoDataFrame.col("userId") &&
        productsPurchasedByUserRddTransformDataFrame.col("productId") === userBuyProductsInfoDataFrame.col("productId") &&
        productsPurchasedByUserRddTransformDataFrame.col("frontMonth") === userBuyProductsInfoDataFrame.col("tradeDate")
      , "left").select(productsPurchasedByUserRddTransformDataFrame.col("userId"), productsPurchasedByUserRddTransformDataFrame.col("productId"),
      productsPurchasedByUserRddTransformDataFrame.col("month"), productsPurchasedByUserRddTransformDataFrame.col("frontMonth"),
      userBuyProductsInfoDataFrame.col("quantity")
    )

    /** **********把DataFramed的销售量为空的替换为0 ***********/
    collectDataFrame = collectDataFrame.na.fill(Map("quantity" -> 0))

    /** *****用户购买产品日期与距离上次购买天数，
      * 如果第一次都买，则上次购买天数为购买时间减去平台上线时间
      */
    /** *******数据格式：用户，产品，交易时间，距离上次交易时间间隔（天） ********/
    userPurchasedProductTimeDataFrame.createGlobalTempView("purchasedTime")
//    val userPurchaseProductInterval = sparkSession.sql("select a.userId as userIdd,a.productId as productIdd,a.purchasedMoth," +
//      " MIN(case when ISNULL(DATEDIFF(a.purchasedStartTime,b.purchaseEndTime)) = false then DATEDIFF(a.purchasedStartTime,b.purchaseEndTime) else DATEDIFF(a.purchasedStartTime,'2017-08-01') END) as purchaseIntervalTime " +
//      "from global_temp.purchasedTime a left join global_temp.purchasedTime b " +
//      " on a.userId = b.userId and a.productId = b.productId and " +
//      " DATE_FORMAT(add_months(CONCAT(a.purchasedMoth,'-','01'),-1),'yyyy-MM') > b.purchasedMoth  " +
//      " GROUP BY a.userId,a.productId,a.purchasedMoth ")
    /****************整合用户产品日期与产品销售日期**********************/
    sparkSession.sql("SELECT a.userId AS userIdd,a.productId AS productIdd,a.month as purchasedMoth,(CASE WHEN ISNULL(b.purchaseEndTime) = FALSE THEN b.purchaseEndTime ELSE DATE_FORMAT(CONCAT(a.month, '-', '28'),'yyyy-MM-dd') END )as purchaseTime  FROM  global_temp.baseData a  LEFT JOIN global_temp.purchasedTime b ON a.userId = b.userId AND a.productId = b.productId AND a.month = b.purchasedMoth ").createGlobalTempView("midData")

    val userPurchaseProductInterval = sparkSession.sql(
      "SELECT a1.userIdd, a1.productIdd, a1.purchasedMoth, MIN(case when ISNULL(DATEDIFF(a1.purchaseTime,b1.purchaseEndTime)) = false then DATEDIFF(a1.purchaseTime,b1.purchaseEndTime) else DATEDIFF(a1.purchaseTime,'2017-08-01') END ) as purchaseIntervalTime " +
        " FROM global_temp.midData a1 LEFT JOIN  global_temp.purchasedTime b1 ON a1.userIdd = b1.userId and a1.productIdd = b1.productId  and a1.purchasedMoth > b1.purchasedMoth" +
        " group by a1.userIdd, a1.productIdd, a1.purchasedMoth"
    )




    /** *********用户购买日期（包括不购买日期）里是否有对应产品活动与用户是否参与了活动 ***************/
    /** *数据格式：用户，产品，每个月份，是否有活动，是否参加活动 ***/
    val userPurchasedProductHasActivity = productsPurchasedByUserRddTransformDataFrame.join(productsActivityDataFrame,
      productsPurchasedByUserRddTransformDataFrame.col("productId") === productsActivityDataFrame.col("productId") &&
        productsPurchasedByUserRddTransformDataFrame.col("month").between(date_format(productsActivityDataFrame.col("activityStartDate"), "yyyy-MM"), date_format(productsActivityDataFrame.col("activityEndDate"), "yyyy-MM")), "left")
      .join(userJoinActivityDataFrame, productsPurchasedByUserRddTransformDataFrame.col("userId") === userJoinActivityDataFrame.col("userId") &&
        productsPurchasedByUserRddTransformDataFrame.col("productId") === userJoinActivityDataFrame.col("productId") &&
        productsPurchasedByUserRddTransformDataFrame.col("month") === userJoinActivityDataFrame.col("dateOfPurchase"), "left")
      .select(productsPurchasedByUserRddTransformDataFrame.col("userId"), productsPurchasedByUserRddTransformDataFrame.col("productId"),
        productsPurchasedByUserRddTransformDataFrame.col("month"), when(isnull(productsActivityDataFrame.col("productId")), 0.0).otherwise(1.0).as("productActivity"),
        when(isnull(userJoinActivityDataFrame.col("productId")), 0.0).otherwise(1.0).as("joinActivity"))


    /** ****指标：当月是否有交易 *************************************/
    /** *******数据格式：用户，产品，月份,上个月交易量,当月是否有交易 *********************/
    var userPurchasedProductRecord = collectDataFrame.join(userBuyProductsInfoDataFrame,
      collectDataFrame.col("userId") === userBuyProductsInfoDataFrame.col("userId") &&
        collectDataFrame.col("productId") === userBuyProductsInfoDataFrame.col("productId") &&
        collectDataFrame.col("month") === userBuyProductsInfoDataFrame.col("tradeDate"), "left")
      .select(collectDataFrame.col("userId"),
        collectDataFrame.col("productId"),
        collectDataFrame.col("month"),
        collectDataFrame.col("quantity"),
        when(isnull(userBuyProductsInfoDataFrame.col("productId")), 0.0).otherwise(1.0).as("purchaseBoolean"))


    /** ***************合并数据集 *******************/
    /** ************多重join，join如果关联格式出现多个相同的列明，后面匹配使用到列明匹配则报
      * Reference '列明' is ambiguous  列明获取定义模糊
      */
    /** **********当月是否有交易，用户，产品，月份,当月是否有活动，当月是否参加活动,上个月交易量,距离上次交易间隔 ********/
    userPurchasedProductRecord.createGlobalTempView("userPurchased")
    userPurchaseProductInterval.createGlobalTempView("userPurchasedInterval")
    userPurchasedProductHasActivity.createGlobalTempView("paroductActivity")
//    var mergeDataframe = sparkSession.sql("select a.userId,a.productId,a.month,a.productActivity,a.joinActivity, " +
//      "b.quantity,b.purchaseBoolean, " +
//      "(case when ISNULL(c.purchaseIntervalTime) = false then c.purchaseIntervalTime  else DATEDIFF(concat(a.month,'-01'),'2017-08-01') END)  as purchaseIntervalTime" +
//      " from  global_temp.paroductActivity a,global_temp.userPurchased b " +
//      " left join global_temp.userPurchasedInterval c on b.userId = c.userIdd and b.productId = c.productIdd and b.month = c.purchasedMoth" +
//      " where a.userId = b.userId and a.productId = b.productId and a.month = b.month ")
    var mergeDataframe = sparkSession.sql("select b.userId,b.productId,b.month,b.quantity,b.purchaseBoolean,c.purchaseIntervalTime " +
  "  from global_temp.userPurchased b " +
  "  left join global_temp.userPurchasedInterval c on b.userId = c.userIdd and b.productId = c.productIdd and b.month = c.purchasedMoth ")

    val whereConditions = whereCondition
    val dateSimpleFormate = new SimpleDateFormat("yyyy-MM")
    val date = new Date()
    val nowDate = dateSimpleFormate.format(date)

    mergeDataframe = mergeDataframe.join(userUuid2Value, "userId").join(productUuid2Value, "productId").where(s"month $whereConditions '2018-07' ")
      .select($"purchaseBoolean", mergeDataframe.col("userId"), mergeDataframe.col("productId"),
        $"userValue", $"productValue", $"month", $"quantity",$"purchaseIntervalTime")
    mergeDataframe
  }
}



object EtlPurchaseRecord{
  def apply: EtlPurchaseRecord = new EtlPurchaseRecord()
}

///*****产品历史活动信息******/
//case class ProductsActivity(var productId:String,
//                            var activityStartDate:String,var activityEndDate:String)
///****用户购买产品的交易信息*****/
//case class UserBuyProductsInfo(var userId:String,var productId:String,
//                               var tradeDate:String,var quantity:Double)
///*****用户参与活动购买产品记录******/
//case class  UserJoinActivity(var userId:String,var productId:String,
//                             var dateOfPurchase:String,var promotionsId:String)
///*******用户每个月购买产品的第一次时间与最后一次时间********/
//case class UserPurchasedProductTime(var userId:String,var productId:String,
//                                    var purchasedMoth:String,var purchasedStartTime:String,var purchaseEndTime:String)
//
//case class LabelaePointData(var purchaseBoolean:Double,
//                            var userValue:Double,var productValue:Double,
//                            var productActivity:Double,var joinActivity:Double,
//                            var quantity:Double)

