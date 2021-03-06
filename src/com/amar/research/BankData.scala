package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object BankData {
  
  var columnNames = Seq("rowId","label","0","1","2","3");
  
  // y = rowId
  // x = 4 columns data-values + label column
  // Return row = "rowId", 23 comma-separated values, label
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, x.split(",")(0).toDouble, x.split(",")(1).toDouble, 
	      x.split(",")(2).toDouble, x.split(",")(3).toDouble, x.split(",")(4).toInt
        )
  }
  
  def getSchema(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("0", DoubleType, true),
	    StructField("1", DoubleType, true),
	    StructField("2", DoubleType, true),
	    StructField("3", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  def mapToDFPredicted(x: String): Row = {
    return Row(
        x.split(",")(0).toInt, x.split(",")(1).toInt, x.split(",")(2).toString()
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