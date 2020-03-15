package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object TestData {
  
  // y = rowId
  // x = 23 columns data-values + label column
  // Return row = "rowId", 23 comma-separated values, label
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, x.split(",")(0).toDouble, x.split(",")(1).toDouble, x.split(",")(2).toDouble, 
	      x.split(",")(3).toDouble, x.split(",")(4).toDouble, x.split(",")(5).toDouble, 
	      x.split(",")(6).toDouble, x.split(",")(7).toDouble, x.split(",")(8).toDouble, 
	      x.split(",")(9).toDouble, x.split(",")(10).toDouble, x.split(",")(11).toDouble, 
	      x.split(",")(12).toDouble, x.split(",")(13).toDouble, x.split(",")(14).toDouble,
	      x.split(",")(15).toDouble, x.split(",")(16).toDouble, x.split(",")(17).toDouble,
	      x.split(",")(18).toDouble, x.split(",")(19).toDouble, x.split(",")(20).toDouble,
	      x.split(",")(21).toDouble, x.split(",")(22).toDouble, x.split(",")(23).toInt
        )
  }
  
  def getTestSchema(): List[StructField] = {
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
	    StructField("9", DoubleType, true),
	    StructField("10", DoubleType, true),
	    StructField("11", DoubleType, true),
	    StructField("12", DoubleType, true),
	    StructField("13", DoubleType, true),
	    StructField("14", DoubleType, true),
	    StructField("15", DoubleType, true),
	    StructField("16", DoubleType, true),
	    StructField("17", DoubleType, true),
	    StructField("18", DoubleType, true),
	    StructField("19", DoubleType, true),
	    StructField("20", DoubleType, true),
	    StructField("21", DoubleType, true),
	    StructField("22", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  
}