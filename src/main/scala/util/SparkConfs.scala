package util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkConfs {
  val sparkConf = new SparkConf()
  sparkConf.setAppName("recommend")
  sparkConf.setMaster("local")
  sparkConf.set("spark.sql.shuffle.partitions","2")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}
