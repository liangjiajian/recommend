package executor

import dataEtl.EtlPurchaseRecorduNew
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.classification.LinearSVC
import util.SparkConfs


object MainObject extends SparkConfs {

  def main(args: Array[String]): Unit = {

    /*************获取的建模数据不包括当月销售数据*************/
    val sourcesDataFrames = new EtlPurchaseRecorduNew().getSourceDataFrame()

//    val split = sourcesDataFrames.randomSplit(Array(0.7,0.3),seed = 11L)
      println("**************"+sourcesDataFrames.count())

    /** *************svm ******************/
   val sourcesDataFrame = sourcesDataFrames.cache()
    val lineSvc = new LinearSVC()
      .setLabelCol("label")
      .setFeaturesCol("features")
        .setMaxIter(130)
      .setRegParam(0.01)
        val linearSVCModel = lineSvc.fit(sourcesDataFrame)

    /*****测试环境路径*****/
    linearSVCModel.write.overwrite().save("D:\\sparkMlModel\\model\\LinearSVC")

    /*******生产环境路径*******/
//    linearSVCModel.write.overwrite().save("/home/liangjiajian/model/LinearSVC")

//    val predictResult  = linearSVCModel.transform(testDataFrame)
//
//        /********结果准确率*******/
//     val (t1,t2,t3) = (predictResult.select("prediction").collect(),predictResult.select("label").collect(),testDataFrame.count().toInt)
//       var  t4 = 0
//       for(i <- 0 to t3-1){
//          if(t1(i)==t2(i)){
//            t4 += 1
//          }
//        }
//
//        val result = 1.0 * t4/t3
//
//        println(s"/**************最终预测结果准确率。。。。。。****$result************/")
//
//    import  sparkSession.implicits._
//    import  org.apache.spark.sql.functions._
//
//    val aaa = predictResult.select(col("features"),col("prediction"),col("label")).rdd.map { col =>
//      val features = col.get(0).asInstanceOf[Vector]
//      val cc = features.toArray
//      val prediction = col.get(1).asInstanceOf[Double]
//      val label = col.get(2).asInstanceOf[Double]
//      new Anc(cc(0).asInstanceOf[Double], cc(1).asInstanceOf[Double],cc(2).asInstanceOf[Double], cc(3).asInstanceOf[Double],prediction,label)
//    }.toDF()
//    aaa.repartition(1).write.csv("D:\\sparkMlModel\\test1")
  }
}

//case class Anc(a:Double,b:Double,c:Double,d:Double,e:Double,f:Double)