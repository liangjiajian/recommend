package executor

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import dataEtl.LinearSVCPredictData
import excutor.MainClass
import executor.MainObject.sparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.sql.SaveMode
import service.DataTreatingServiceImpl
import util.SparkConfs

object PredictMainObject extends SparkConfs{
  def main(args: Array[String]): Unit = {

    /*****测试环境路径*****/
    val linearSVCModel = LinearSVCModel.load("D:\\sparkMlModel\\model\\LinearSVC")

    /*******生产环境路径*******/
//    val linearSVCModel = LinearSVCModel.load("/home/liangjiajian/model/LinearSVC")


    val (sources,userUuid2Value,productUuid2Value) = LinearSVCPredictData.apply.getDataSource()

    import  sparkSession.implicits._

    val sc = sparkSession.sparkContext

    val sourcesDataFrame = sources.zipWithUniqueId().toDF("features","idx").cache()

    linearSVCModel.setFeaturesCol("features")
    linearSVCModel.setPredictionCol("prediction")

    import  org.apache.spark.sql.functions._

    val resultDataFrame = linearSVCModel.transform(sourcesDataFrame)


    val predictResult = resultDataFrame.select(col("features"),col("prediction")).rdd.map { col =>
      val features = col.get(0).asInstanceOf[Vector]
      val array = features.toArray
      val prediction = col.get(1).asInstanceOf[Double]
      (array(0),array(1),prediction)
    }.filter(col=>col._3==1.0).map(col=>(col._1,col._2)).toDF("userValue","productValue")


    val mainClass = new MainClass()
    val dataTreatingServiceImpl: DataTreatingServiceImpl = mainClass.getDataTreatingServiceImpl

    import scala.collection.JavaConverters._

    /**********根据用户近半年购买产品的交易次数与销量排序分类的结果**********/
    val userPurchases = sc.parallelize(dataTreatingServiceImpl.getUserPurchasedProductQuantity.asScala).map{col=>
      val userId = col.get("userId").toString
      val productId = col.get("prodcutId").toString
      val orders = col.get("orders").asInstanceOf[Long]
      val quantity = scala.math.BigDecimal(col.get("quantity").asInstanceOf[BigDecimal]).toInt
      (userId,productId,orders,quantity)
    }.toDF("userId","productId","orders","quantity")

    /***********把uservalue与productvalue转换成userID和productID***********/
    val real = predictResult.join(userUuid2Value,predictResult.col("userValue")===userUuid2Value("userValue")).join(productUuid2Value,predictResult.col("productValue")===productUuid2Value("productValue")).select(userUuid2Value.col("userId"),productUuid2Value("productId"))

    /**************推荐数据执行完毕的时间*****************/
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val  nowDate = new Date()
    val  updateDate = dateFormat.format(nowDate)

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val endResult = real.join(userPurchases,Array("userId","productId")).orderBy($"userId",$"orders".desc,$"quantity".desc)
      .select(col("userId"),col("productId").as("product_sku_uuid"),when(isnull(col("productId")),updateDate).otherwise(updateDate).as("updateDate"))


    val properties = new Properties()
    properties.setProperty("user","hive")
    properties.setProperty("password","Neter!!2018")
    properties.setProperty("driver","com.mysql.jdbc.Driver")

    endResult.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://123.207.40.137:3306/rocket?useUnicode=true&characterEncoding=utf-8&useSSL=false","rocket.recommend_product_sku",properties)
  }
}

case class  HistoricalSales(userId:String,productId:String,orders:Long)
