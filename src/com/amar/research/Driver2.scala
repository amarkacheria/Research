package com.amar.research;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable;
import com.amar.research.{ItemsMapTrans};
import scala.collection._;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;
import org.apache.spark
import org.apache.spark;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import scala.sys.process.stringToProcess;

import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance};
import com.amar.research.Process.outputFileLocation;

object Process2 extends App with Context {

  import sparkSession.implicits._;
  // Configuration
	val inputFileLocation = "src/resources/bank-data/bank-data-normalized-ceilinged-2to-2-output2/*.csv";
	println(inputFileLocation);
	val output2FileLocation = inputFileLocation.substring(0, inputFileLocation.length()-4) + "-driver2-output";

  // Read Data
	val predictedData = sparkSession.sparkContext.textFile(inputFileLocation);
	predictedData.take(5).map(println);
	
	// Convert predicted rows to DataFrame for easier manipulation
	val fileToDf = predictedData.map{ case(x) => BankData.mapToDFPredicted(x)};
	val schema = BankData.getBankSchemaPredicted();
	val df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));
	
	df.show();
	// runs command fine
	// output is printed but needs to be parsed out and used. 
  var errorFound = false;
  var fileCounter = 1;
  while (!errorFound) {
    try {
      val proc = stringToProcess("cmd /C trimax ./src/resources/bank-data/csv/" + fileCounter + "/*.csv 0.5 10 1 200 4");
      // check the top bottom labels 
      println(proc.!!)
      fileCounter += 1;
    } catch {
      case e: Exception => {
        errorFound = true;
      }
    }
  }
	
  println("Done");
	
	
	//-----------------------------------------------------------------------------------
   
}