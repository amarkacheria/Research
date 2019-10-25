package com.amar.research.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.amar.research.ItemsMapTrans
import com.amar.research.ItemSet


trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("Amar Research")
    .setMaster("local[*]")
//    .set("spark.executor.memory", "1g")
//    .set("spark.driver.memory", "4g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sparkConf.registerKryoClasses(Array(classOf[ItemSet], classOf[ItemsMapTrans]))
  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
    
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
}
