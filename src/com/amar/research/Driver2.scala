package com.amar.research;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable;
import scala.collection._;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;
import org.apache.spark
import org.apache.spark;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.rdd.RDD;
import scala.sys.process.stringToProcess;
import java.io._;

import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance };
import com.amar.research.Process.outputFileLocation;
import org.apache.spark.SparkContext

object Process2 extends App with Context {

  import sparkSession.implicits._;
  // Parameters
  var theta = 0.1;   // 0.05
  var minRows = 10;    // 4
  var maxRows = 100;
  var minCols = 1;
  
  // Configuration
  val inputFileLocation = "src/resources/bank-data/output/*.csv";
  println(inputFileLocation);
  val output2FileLocation = "src/resources/bank-data/output/trimax";
  val trainingLabelsLocation = "src/resources/bank-data/output/training-labels.txt";

  // Read Data
  val predictedData = sparkSession.sparkContext.textFile(inputFileLocation);

  // Convert predicted rows to DataFrame for easier manipulation
  val fileToDf = predictedData.map { case (x) => BankData.mapToDFPredicted(x) };
  val schema = BankData.getBankSchemaPredicted();
  var df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));

  // For some reason, reading empty strings (added in driver1) is a bit weird
  // need to check for escaped quotes. 
  df = df.withColumn(
    "predicted",
    when(
      df.col("predicted").equalTo("\"\""),
      lit(""))
      .otherwise(df.col("predicted")));
  
  var dfHashMap = df.rdd.map{
    case Row(rowNum: Int, col0: Double, col1: Double, col2: Double, col3: Double, label: Int, predicted: String) => (rowNum, predicted)
  }.collectAsMap();
  
  val mutablePredictedMap = mutable.Map(dfHashMap.toSeq: _*);
  val trainingLabels = sparkSession.sparkContext.textFile(trainingLabelsLocation).collect();
  
	var trainingLabelsSet = mutable.Set(trainingLabels: _*);
	println(trainingLabelsSet.size);
	trainingLabelsSet.map(print);
	
  // runs command fine
  // output is printed but needs to be parsed out and used.
  //  var errorFound = false;
  var fileCounter = 1;
  var minColsCounter = 3;
  while (minColsCounter >= minCols) {
    try {
//      if (minColsCounter == 1) {
//        minRows = 15;
//      }
      println("File: " + fileCounter);
      val proc = stringToProcess("cmd /C trimax ./src/resources/bank-data/csv/" + fileCounter + "/*.csv " + theta + " " + minRows + " " + minColsCounter + " " + maxRows + " " + minColsCounter);
      // check the top bottom labels
      val result = proc.!!;
      println(result);
      if (result.trim().length() > 0) {
        val origLines = result.split("\n");
//        val distLines = sparkSession.sparkContext.parallelize(lines, 25);
//        println(distLines.count());
        val lines = origLines.sortBy((line) => {
          line.split(" -----").apply(0).split(" ").length
        })(Ordering.Int.reverse)
        lines.foreach((line) => {
          println(line);
          val rowNums = line.split(" -----").apply(0).split(" ").map(_.toInt).toSeq;
          var colNamesStr = "rowId " + line.split(" -----").apply(1) + "label predicted";
          colNamesStr = colNamesStr.split('\n').map(_.trim.filter(_ >= ' ')).mkString;
          val colNames = colNamesStr.split(" ");

          val bicCols = colNames.drop(1).dropRight(1).dropRight(1);
          // Create new DF with rows from this bicluster
          val rowsDf = df.where($"rowId" isin (rowNums: _*));
          // Keep only columns which are part of this bicluster in the DF, rowId, and label
          val colDf = rowsDf.select(colNames.head, colNames.tail: _*);
          var labelsDifferent = 0;
          var labelsSame = 0;
	        var labelsSameLabels =  mutable.ListBuffer[String]();
          //    	  colDf.show();
	        
	        val rowsLength = rowNums.length;
	        var unProcessedNum = 0;
	        rowNums.foreach((rowNum) => {
	          val rowPrediction = String.valueOf(mutablePredictedMap.get(rowNum).getOrElse("none"));
	          if (rowPrediction == "" || rowPrediction == "?" || rowPrediction == "none") {
	            unProcessedNum = unProcessedNum + 1;
	          }
	        });
	        var isProcessed = true;
          bicCols.foreach(colName => {
            val colSortedDF = colDf.orderBy(asc(colName));
            val withId = colSortedDF.withColumn("_id", monotonically_increasing_id()).orderBy("_id");
            //    	    withId.show();
            val firstRow = withId.head(1).apply(0);
            val secondRow = withId.head(2).apply(1);
            val lastRow = withId.orderBy(desc("_id")).head(1).apply(0);
            val secondLastRow = withId.orderBy(desc("_id")).head(2).apply(1);
            
            if (trainingLabelsSet.contains(firstRow.getAs[String]("rowId")) && 
                trainingLabelsSet.contains(secondRow.getAs[String]("rowId")) &&
                trainingLabelsSet.contains(lastRow.getAs[String]("rowId")) &&
                trainingLabelsSet.contains(secondLastRow.getAs[String]("rowId"))) {
               isProcessed = false;              
            } else {
              	if (unProcessedNum/rowsLength > 0.20) {
      	          isProcessed = false;
      	        }
            }
              
            if (!isProcessed) {
          	  trainingLabelsSet.add(firstRow.getAs[String]("rowId"));
          	  trainingLabelsSet.add(secondRow.getAs[String]("rowId"));
          	  trainingLabelsSet.add(lastRow.getAs[String]("rowId"));
          	  trainingLabelsSet.add(secondLastRow.getAs[String]("rowId"));
              //      	  println(firstRow.mkString(" "));
              //      	  println(secondRow.mkString(" "));
              //      	  println(lastRow.mkString(" "));
              //      	  println(secondLastRow.mkString(" "));
              //      	  println("First row label: " + firstRow.getAs[String]("label") + " when sorting by column: " + colName);
              //      	  println("Second row label: " + secondRow.getAs[String]("label") + " when sorting by column: " + colName);
              //      	  println("Last row label: " + lastRow.getAs[String]("label") + " when sorting by column: " + colName);
              //      	  println("Second Last row label: " + secondLastRow.getAs[String]("label") + " when sorting by column: " + colName);
  
              if (firstRow.getAs[String]("label") != lastRow.getAs[String]("label") ||
                firstRow.getAs[String]("label") != secondRow.getAs[String]("label") ||
                firstRow.getAs[String]("label") != secondLastRow.getAs[String]("label")) {
                println("LABELS ARE DIFFERENT!!")
                labelsDifferent = labelsDifferent + 1;
              } else {
                println("LABELS ARE SAME!!!");
                labelsSame = labelsSame + 1;
    	          labelsSameLabels += String.valueOf(firstRow.getAs[String]("label"));
              }
            }
          });
  
          if (!isProcessed) {
            println("Labels Same: " + labelsSame + "    Labels Different: " + labelsDifferent);
            println(" ");
            
            var allLabelsSame = false;
        	  if (labelsSameLabels.toList.exists(_ != labelsSameLabels.toList.head)) {
        	    allLabelsSame = false;
        	  } else {
        	    allLabelsSame = true;
        	  }
            if (labelsSame > labelsDifferent && allLabelsSame) {
  //              df = df.withColumn(
  //                "predicted",
  //                when(($"rowId" isin (rowNums: _*)) && ($"predicted"===lit("?")),
  //                  lit(colDf.head().getAs[String]("label")))
  //                  .otherwise(df.col("predicted")));
  //              df.unpersist();
  //              df.cache();
              
              rowNums.foreach(rowId => {
                  mutablePredictedMap.update(rowId, colDf.head().getAs[String]("label"));
              });
              
            }
          } else {
            println("Rows Already Processed");
          }
        });
      }
      fileCounter = fileCounter + 1;
      System.gc(); 
    } catch {
      case runtimeException: RuntimeException => {
        //        errorFound = true;
        println("RunTimeException for file: " + fileCounter);
        runtimeException.printStackTrace();
        minColsCounter = minColsCounter - 1;
        fileCounter = 1;
        println("reducing minCols to : " + minColsCounter);
      }
      case e: Exception => {
        e.printStackTrace();
      }
    }
  }
  
  val updatedRDDMap = df.rdd.map{
    case Row(rowNum: Int, col0: Double, col1: Double, col2: Double, col3: Double, label: Int, predicted: String) => {
      Row(rowNum, col0, col1, col2, col3, label, String.valueOf(mutablePredictedMap.get(rowNum).get))
    }
  }
  
  println("updatedRDDMap done");
  
  val finalDf = sparkSession.createDataFrame(updatedRDDMap, org.apache.spark.sql.types.StructType(BankData.getBankSchemaPredicted()));

  finalDf.show();
  
  val predictionAndLabels = finalDf.select("label", "predicted").map(row => {
    var prediction = row.getAs[String](1);
    if (prediction == "?") prediction = "99.0";
    else if (prediction == "") prediction = "100.0";

    val doublePrediction = prediction.toDouble;

    (doublePrediction, row(0).asInstanceOf[Int].toDouble)
  }).rdd
  
  // represents "?" and "" (empty string) which are not there in dataset but predictions may contain those characters. 
  val additionalLabels: RDD[(Double, Double)] = sparkSession.sparkContext.parallelize(Seq((99.0, 99.0), (100.0, 100.0)));
	  
  val metrics = new MulticlassMetrics(predictionAndLabels.++(additionalLabels));
  println("Confusion matrix:")
  println(metrics.confusionMatrix)
  
  finalDf.coalesce(1).write.mode(SaveMode.Overwrite).csv(output2FileLocation);
  val pw = new PrintWriter(new File(output2FileLocation + "/confusion-matrix.txt" ));
  pw.write(metrics.labels.map(_.toString).mkString(","))
  pw.write("\r\n");
  pw.write("\r\n");
  pw.write("------------------------------------------- \r\n");
  pw.write(metrics.confusionMatrix.toString)
  pw.write("\r\n");
  pw.write("------------------------------------------- \r\n");
  pw.close();

  val pw1 = new PrintWriter(new File(output2FileLocation + "/training-labels.txt" ));
  pw1.write(trainingLabelsSet.mkString("\n"));
  pw1.close();
  println("Done");

  //-----------------------------------------------------------------------------------

}