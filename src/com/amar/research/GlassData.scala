package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object GlassData {
  
  // y = rowId
  // x = 23 columns data-values + label column
  // Return row = "rowId", 23 comma-separated values, label
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, x.split(",")(1).toDouble, x.split(",")(2).toDouble, 
	      x.split(",")(3).toDouble, x.split(",")(4).toDouble, x.split(",")(5).toDouble, 
	      x.split(",")(6).toDouble, x.split(",")(7).toDouble, x.split(",")(8).toDouble, 
	      x.split(",")(9).toDouble, x.split(",")(10).toInt
        )
  }
  
  def getGlassSchema(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("0", DoubleType, true),
	    StructField("1", DoubleType, true),
	    StructField("2", DoubleType, true),
	    StructField("3", DoubleType, true),
	    StructField("4", DoubleType, true),
	    StructField("5", DoubleType, true),
	    StructField("6", DoubleType, true),
	    StructField("7", DoubleType, true),
	    StructField("8", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  
}