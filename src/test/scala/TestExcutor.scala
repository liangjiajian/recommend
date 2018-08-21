import excutor.MainClass
import service.DataTreatingServiceImpl
import java.math.BigDecimal
import java.util.Properties

import org.apache.spark.sql.SaveMode
import util.SparkConfs

object TestExcutor extends SparkConfs{

  def main(args: Array[String]): Unit = {

    import scala.collection.JavaConverters._
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    val rdd = sc.textFile("D:\\sparkMlModel\\test1\\part-00000-14ed49f4-9277-4b71-a90b-9c2d231d5c6d-c000.csv")
    val predictResult = rdd.map(line=>line.split(",")).filter(col=>col(4).toDouble==1.0)
      .map{col=>
        val userValue = col(0).toDouble
        val productValue = col(1).toDouble
        (userValue,productValue)
      }.toDF("userValue","productValue")

    val mainClass = new MainClass()
    val dataTreatingServiceImpl: DataTreatingServiceImpl = mainClass.getDataTreatingServiceImpl

    val userUuid2Value = sc.parallelize(dataTreatingServiceImpl.getUserId2UserValue.asScala).map{
      col=> val userId = col.get("userId").toString
        val uservalue = col.get("uservalue").asInstanceOf[Double]
        (userId,uservalue)
    }.toDF("userId","userValue")


    val productUuid2Value = sc.parallelize(dataTreatingServiceImpl.getProduct2ProductValue.asScala).map{
      col=> val userId = col.get("prodcutId").toString
        val uservalue = col.get("prodcutValue").asInstanceOf[Double]
        (userId,uservalue)
    }.toDF("productId","productValue")

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

    val endResult = real.join(userPurchases,Array("userId","productId")).orderBy($"userId",$"orders".desc,$"quantity".desc)
      .select("userId","productId")

    val properties = new Properties()
    properties.setProperty("user","hive")
    properties.setProperty("password","Neter!!2018")
    properties.setProperty("driver","com.mysql.jdbc.Driver")

    endResult.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://123.207.40.137:3306/rocket?useUnicode=true&characterEncoding=utf-8","rocket.recommendProduct",properties)
  }
}
