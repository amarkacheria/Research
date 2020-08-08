package com.amar.research;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable;
import com.amar.research.{ ItemsMapTrans };
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
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance };
import com.amar.research.Process.outputFileLocation;
import org.apache.spark.SparkContext

object Process2 extends App with Context {

  import sparkSession.implicits._;
  // Configuration
  val inputFileLocation = "src/resources/bank-data/bank-data-normalized-ceilinged-2to-2-output2/*.csv";
  println(inputFileLocation);
  val output2FileLocation = inputFileLocation.substring(0, inputFileLocation.length() - 6) + "-driver2-output";

  // Read Data
  val predictedData = sparkSession.sparkContext.textFile(inputFileLocation);

  // Convert predicted rows to DataFrame for easier manipulation
  val fileToDf = predictedData.map { case (x) => BankData.mapToDFPredicted(x) };
  val schema = BankData.getBankSchemaPredicted();
  var df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));

  df = df.withColumn(
    "predicted",
    when(
      df.col("predicted").equalTo("\"\""),
      lit("-"))
      .otherwise(df.col("predicted")));

  // runs command fine
  // output is printed but needs to be parsed out and used.
  //  var errorFound = false;
  var fileCounter = 1;
  var minColsCounter = 1;
  while (minColsCounter >= 1) {
    try {
      println("File: " + fileCounter);
      val proc = stringToProcess("cmd /C trimax ./src/resources/bank-data/csv/" + fileCounter + "/*.csv 0.4 10 " + minColsCounter + " 50 4");
      // check the top bottom labels
      val result = proc.!!;
      println(result);
      if (result.trim().length() > 0) {
        val lines = result.split("\n");
//        val distLines = sparkSession.sparkContext.parallelize(lines, 25);
//        println(distLines.count());
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
          //    	  colDf.show();

          bicCols.foreach(colName => {
            val colSortedDF = colDf.orderBy(asc(colName));
            val withId = colSortedDF.withColumn("_id", monotonically_increasing_id()).orderBy("_id");
            //    	    withId.show();
            val firstRow = withId.head(1).apply(0);
            val secondRow = withId.head(2).apply(1);
            val lastRow = withId.orderBy(desc("_id")).head(1).apply(0);
            val secondLastRow = withId.orderBy(desc("_id")).head(2).apply(1);
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

            }
          });

          println("Labels Same: " + labelsSame + "    Labels Different: " + labelsDifferent);
          println(" ");
          if (labelsSame > labelsDifferent) {
              df = df.withColumn(
                "predicted",
                when(($"rowId" isin (rowNums: _*)) && ($"predicted"===lit("?")),
                  lit(colDf.head().getAs[String]("label")))
                  .otherwise(df.col("predicted")));
              df.unpersist();
              df.cache();
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

  df.show(false);

  val predictionAndLabels = df.select("label", "predicted").map(row => {
    var prediction = row.getAs[String](1);
    if (prediction == "?") prediction = "99.0";
    else if (prediction == "") prediction = "100.0";
    else if (prediction == "-") prediction = "101.0";

    val doublePrediction = prediction.toDouble;

    (doublePrediction, row(0).asInstanceOf[Int].toDouble)
  }).rdd

  val metrics = new MulticlassMetrics(predictionAndLabels);
  println("Confusion matrix:")
  println(metrics.confusionMatrix)
  df.coalesce(1).write.mode(SaveMode.Overwrite).csv(output2FileLocation);

  println("Done");

  //-----------------------------------------------------------------------------------

}