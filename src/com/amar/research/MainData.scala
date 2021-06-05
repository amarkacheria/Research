package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

//	val minSupport = 1000; // For Charm
//	val minSupportCol = 1; // For filtering concepts
//	val bicValidation = 0.05; // Check 5% of rows from top and bottom for labels
//	val folder = "skin-data";
//	val inputFileLocation1 = "src/resources/" + folder + "/skin-data.csv";
//	val folderLocation = "src/resources/" + folder;
//	val outputFileLocation = folderLocation + "/output";
//	val trange = getTRange(0.0, 255.0, 5, 0.05);

object MainData {
  
  var columnNames = Seq("rowId","label","0","1","2");
  
  def mapToDF(x: String, y: Long): Row = {
    return Row(
        y.toInt, 
        x.split(",")(0).toDouble, 
        x.split(",")(1).toDouble, 
	      x.split(",")(2).toDouble,
	      x.split(",")(3).toInt
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