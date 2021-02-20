package com.amar.research

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;

object DigitsData {
  
  // y = rowId
  // x = 4 columns data-values + label column
  // Return row = "rowId", 23 comma-separated values, label
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
	      x.split(",")(34).toDouble, 
	      x.split(",")(35).toDouble, 
	      x.split(",")(36).toDouble, 
        x.split(",")(37).toDouble, 
	      x.split(",")(38).toDouble, 
	      x.split(",")(39).toDouble,
	      x.split(",")(40).toDouble, 
        x.split(",")(41).toDouble, 
	      x.split(",")(42).toDouble, 
	      x.split(",")(43).toDouble, 
	      x.split(",")(44).toDouble, 
        x.split(",")(45).toDouble, 
	      x.split(",")(46).toDouble, 
	      x.split(",")(47).toDouble,
	      x.split(",")(48).toDouble, 
        x.split(",")(49).toDouble, 
	      x.split(",")(50).toDouble, 
	      x.split(",")(51).toDouble, 
	      x.split(",")(52).toDouble, 
        x.split(",")(53).toDouble, 
	      x.split(",")(54).toDouble, 
	      x.split(",")(55).toDouble,
	      x.split(",")(56).toDouble, 
        x.split(",")(57).toDouble, 
	      x.split(",")(58).toDouble, 
	      x.split(",")(59).toDouble, 
	      x.split(",")(60).toDouble, 
        x.split(",")(61).toDouble, 
	      x.split(",")(62).toDouble, 
	      x.split(",")(63).toDouble,
	      x.split(",")(64).toInt
        )
  }
  
//  def mapToDF(x: String, y: Long): Row = {
//    return Row(
//        y.toInt, x.split(",")(0).toDouble, x.split(",")(4).toInt
//        )
//  }
//  
  def getDigitsSchema(): List[StructField] = {
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
	    StructField("34", DoubleType, true),
	    StructField("35", DoubleType, true),
	    StructField("36", DoubleType, true),
	    StructField("37", DoubleType, true),
	    StructField("38", DoubleType, true),
	    StructField("39", DoubleType, true),
	    StructField("40", DoubleType, true),
	    StructField("41", DoubleType, true),
	    StructField("42", DoubleType, true),
	    StructField("43", DoubleType, true),
	    StructField("44", DoubleType, true),
	    StructField("45", DoubleType, true),
	    StructField("46", DoubleType, true),
	    StructField("47", DoubleType, true),
	    StructField("48", DoubleType, true),
	    StructField("49", DoubleType, true),
	    StructField("50", DoubleType, true),
	    StructField("51", DoubleType, true),
	    StructField("52", DoubleType, true),
	    StructField("53", DoubleType, true),
	    StructField("54", DoubleType, true),
	    StructField("55", DoubleType, true),
	    StructField("56", DoubleType, true),
	    StructField("57", DoubleType, true),
	    StructField("58", DoubleType, true),
	    StructField("59", DoubleType, true),
	    StructField("60", DoubleType, true),
	    StructField("61", DoubleType, true),
	    StructField("62", DoubleType, true),
	    StructField("63", DoubleType, true),
	    StructField("label", IntegerType, true)
	  )
  }
  
  def mapToDFPredicted(x: String): Row = {
    return Row(
        x.split(",")(0).toInt, x.split(",")(1).toDouble, x.split(",")(2).toDouble, 
	      x.split(",")(3).toDouble, x.split(",")(4).toDouble, x.split(",")(5).toInt, x.split(",")(6).toString()
        )
  }
  
//  def mapToDFPredicted(x: String): Row = {
//    return Row(
//        x.split(",")(0).toInt, x.split(",")(1).toDouble, x.split(",")(5).toInt, x.split(",")(6).toString()
//        )
//  }
  
  def getDigitsSchemaPredicted(): List[StructField] = {
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
	    StructField("34", DoubleType, true),
	    StructField("35", DoubleType, true),
	    StructField("36", DoubleType, true),
	    StructField("37", DoubleType, true),
	    StructField("38", DoubleType, true),
	    StructField("39", DoubleType, true),
	    StructField("40", DoubleType, true),
	    StructField("41", DoubleType, true),
	    StructField("42", DoubleType, true),
	    StructField("43", DoubleType, true),
	    StructField("44", DoubleType, true),
	    StructField("45", DoubleType, true),
	    StructField("46", DoubleType, true),
	    StructField("47", DoubleType, true),
	    StructField("48", DoubleType, true),
	    StructField("49", DoubleType, true),
	    StructField("50", DoubleType, true),
	    StructField("51", DoubleType, true),
	    StructField("52", DoubleType, true),
	    StructField("53", DoubleType, true),
	    StructField("54", DoubleType, true),
	    StructField("55", DoubleType, true),
	    StructField("56", DoubleType, true),
	    StructField("57", DoubleType, true),
	    StructField("58", DoubleType, true),
	    StructField("59", DoubleType, true),
	    StructField("60", DoubleType, true),
	    StructField("61", DoubleType, true),
	    StructField("62", DoubleType, true),
	    StructField("63", DoubleType, true),
	    StructField("label", IntegerType, true),
	    StructField("predicted", StringType, true)
	  )
  }
  
   def getDigitsSchemaPredicted2(): List[StructField] = {
    return List(
	    StructField("rowId", IntegerType, true),
	    StructField("label", IntegerType, true),
	    StructField("predicted", StringType, true)
	  )
  }
  
}