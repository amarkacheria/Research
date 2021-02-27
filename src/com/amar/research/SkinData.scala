package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object SkinData {
  
  // y = rowId
  // x = 23 columns data-values + label column
  // Return row = "rowId", 23 comma-separated values, label
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, x.split(",")(0).toDouble, x.split(",")(1).toDouble, 
	      x.split(",")(2).toDouble, x.split(",")(3).toInt
        )
  }
  
  def getSkinSchema(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("0", DoubleType, true),
	    StructField("1", DoubleType, true),
      StructField("2", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  def mapToDFPredicted(x: String): Row = {
    return Row(
        x.split(",")(0).toInt, x.split(",")(1).toDouble, x.split(",")(2).toDouble, 
	      x.split(",")(3).toDouble, x.split(",")(4).toInt, x.split(",")(5).toString()
        )
  }
  
  def getSkinSchemaPredicted(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("0", DoubleType, true),
	    StructField("1", DoubleType, true),
	    StructField("2", DoubleType, true),
	    StructField("label", IntegerType, true),
	    StructField("predicted", StringType, true)
	  )
  }
  
}