import excutor.MainClass
import util.SparkConfs

object TestObject2 extends SparkConfs{

  def main(args: Array[String]): Unit = {

    val sc = sparkSession.sparkContext

    val mainClass = new MainClass
    import scala.collection.JavaConverters._
    val userPurchaseDate = mainClass.getDataTreatingServiceImpl.getUserPurchaseDate.asScala

    import sparkSession.implicits._
    val purDateDF = sc.parallelize(userPurchaseDate).map{col=>
      val userId = col.get("userId").toString
      val productId = col.get("productId").toString
      val purDate = col.get("saleDate").toString
      (userId,productId,purDate)
    }.toDF("userId","productId","purDate").cache()

    purDateDF.createTempView("purDate")

    sparkSession.sql("select a1.userId,a1.productId,Round(avg(a1.intervals)) as avgIntervals  from " +
      " (select a.userId,a.productId,a.purDate,min(DATEDIFF(a.purDate,b.purDate)) as intervals from " +
      " purDate a,purDate b " +
      " where a.userId = b.userId and a.productId = b.productId and a.purDate > b.purDate " +
      " group by a.userId,a.productId,a.purDate ) a1 " +
      " group by a1.userId,a1.productId" )

  }
}
