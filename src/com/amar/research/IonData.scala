package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object IonData {
  
  var columnNames = Seq("rowId","label","0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33");
  
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
	      x.split(",")(7).toDouble, 
	      x.split(",")(8).toDouble, 
	      x.split(",")(9).toDouble, 
	      x.split(",")(10).toDouble,
        x.split(",")(11).toDouble, 
	      x.split(",")(12).toDouble, 
	      x.split(",")(13).toDouble, 
	      x.split(",")(14).toDouble, 
	      x.split(",")(15).toDouble,
	      x.split(",")(16).toDouble, 
	      x.split(",")(17).toDouble, 
	      x.split(",")(18).toDouble, 
	      x.split(",")(19).toDouble, 
	      x.split(",")(20).toDouble,
	      x.split(",")(21).toDouble, 
	      x.split(",")(22).toDouble, 
	      x.split(",")(23).toDouble, 
	      x.split(",")(24).toDouble, 
	      x.split(",")(25).toDouble,
	      x.split(",")(26).toDouble, 
	      x.split(",")(27).toDouble, 
	      x.split(",")(28).toDouble, 
	      x.split(",")(29).toDouble, 
	      x.split(",")(30).toDouble,
	      x.split(",")(31).toDouble,
	      x.split(",")(32).toDouble,
	      x.split(",")(33).toDouble,
	      x.split(",")(34).toInt
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
	    StructField("23", DoubleType, true),
	    StructField("24", DoubleType, true),
	    StructField("25", DoubleType, true),
	    StructField("26", DoubleType, true),
	    StructField("27", DoubleType, true),
	    StructField("28", DoubleType, true),
	    StructField("29", DoubleType, true),
	    StructField("30", DoubleType, true),
	    StructField("31", DoubleType, true),
	    StructField("32", DoubleType, true),
	    StructField("33", DoubleType, true),
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