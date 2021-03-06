package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object RiceData {
  
  var columnNames = Seq("rowId","label","0","1","2","3","4","5","6");
  
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, 
        x.split(",")(0).toDouble, 
        x.split(",")(1).toDouble, 
	      x.split(",")(2).toDouble, 
	      x.split(",")(3).toDouble, 
	      x.split(",")(4).toDouble, 
	      x.split(",")(5).toDouble,
	      x.split(",")(6).toDouble,
	      x.split(",")(7).toInt
        )
  }
  
  def mapToDFPredicted(x: String): Row = {
    return Row(
        x.split(",")(0).toInt, x.split(",")(1).toInt, x.split(",")(2).toString()
        )
  }
  
  def getSchema(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("0", DoubleType, true),
	    StructField("1", DoubleType, true),
	    StructField("2", DoubleType, true),
	    StructField("3", DoubleType, true),
	    StructField("4", DoubleType, true),
	    StructField("5", DoubleType, true),
	    StructField("6", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  def getSchemaPredicted(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("label", IntegerType, true),
	    StructField("predicted", StringType, true)
	  )
  }
  
}