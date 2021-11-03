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

import java.util.Calendar;
import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance };
import org.apache.spark.SparkContext
import scala.util.control.Breaks._;
import com.amar.research.Process.folderLocation;

object Process2 extends App with Context {
	// find a way to add max cols from driver 1 so minColsCounter can be set accordingly in driver2
	import sparkSession.implicits._;
	// Parameters
	val theta = 0.15;
	val minRows = 10;
	val maxRows = 100000;
	val minCols = 1;
	val bicValidation = 0.05;
	val lastFile = 43;
   
	// Configuration
	val folderLocation = "src/resources/bank-data";
	println(folderLocation);
	// val inputFileLocation = "src/resources/" + folder + "/output/*.csv";
	val inputFileLocation = folderLocation + "/output/*.csv";
	val output2FileLocation = folderLocation + "/output/trimax";
	val trainingLabelsLocation = folderLocation + "/output/training-labels.txt";
	val confusionMatrixLocation2 = output2FileLocation + "/confusion-matrix-trimax.txt";
	val trainingLabelsLocation2 = output2FileLocation + "/training-labels-trimax.txt";
	var minColsCounter = 4;

	val startTime = Calendar.getInstance().getTime();
	
	// Read Data
	val predictedData = sparkSession.sparkContext.textFile(inputFileLocation);

	// Convert predicted rows to DataFrame for easier manipulation
	val fileToDf = predictedData.map { case (x) => MainData.mapToDFPredicted(x) };
	val schema = MainData.getSchemaPredicted();
	var df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));

	// For some reason, reading empty strings (added in driver1) is a bit weird
	// need to check for escaped quotes. 
	df = df.withColumn("predicted", when(df.col("predicted").equalTo("\"\""),lit("")).otherwise(df.col("predicted")));
	
	var dfHashMap = df.rdd.map{
		case Row(rowNum: Int, label: Int, predicted: String) => (rowNum, predicted)
	}.collectAsMap();
	
	var dfLabelHashMap = df.rdd.map{
		case Row(rowNum: Int, label: Int, predicted: String) => (rowNum, label)
	}.collectAsMap();
  
	val mutablePredictedMap = mutable.ListMap(dfHashMap.toSeq: _*);
	val trainingLabels = sparkSession.sparkContext.textFile(trainingLabelsLocation).collect();
	
	val trainingLabelsInt = trainingLabels.map((label) => label.toInt);
	var trainingLabelsSet = mutable.Set(trainingLabelsInt: _*);
	println(trainingLabelsSet.size);
	
	var fileCounter = 1;
	val trimaxDF = mutable.ListMap[String, String]();
  	while (minColsCounter >= minCols) {
		val file: File = new File( folderLocation + "/csv/" + fileCounter);
		println("File: " + fileCounter + " exists: " + file.exists());
		if (file.exists()) {
			val proc = stringToProcess("cmd /C trimax ./" + folderLocation + "/csv/" + fileCounter + "/*.csv " + theta + " " + minRows + " " + minColsCounter + " " + maxRows + " " + minColsCounter);
			println("output " + fileCounter);
			var result = "";
//			result = proc.!!;
//			Thread.sleep(1000);
			try {
				result = proc.!!;
			} catch {
				case e: RuntimeException => println("RuntimeException");
				e.printStackTrace();
				Thread.sleep(1000);
				fileCounter = fileCounter - 1;
			}
			
			println("result: " + result);
			if (result.trim().length() > 0) {
				var bicString ="";
				var labelRow = "";
				val bicData = sparkSession.sparkContext.textFile("./" + folderLocation + "/csv/" + fileCounter + "/*.csv")
				.collect()
				.zipWithIndex.map((s) => {
					if (s._2 != 0) {
						bicString = bicString + s._1.toString() + "#";
					} else {
						labelRow = s._1.toString()
					}
				});
				trimaxDF.update(fileCounter + "-" + labelRow + "-" + bicString, result);
			}
      	}
		fileCounter = fileCounter + 1;
		if (fileCounter > lastFile) {
			minColsCounter = minColsCounter - 1;
			fileCounter = 1;
			println("reducing minCols to : " + minColsCounter);
		}
  	}
  
  	val parallelTrimaxDF = sparkSession.sparkContext.parallelize(trimaxDF.toSeq);
  
  	val predictedResultsRDD = parallelTrimaxDF.map((result) => {
    
		val origLines = result._2.split("\n");
		val fileCount = result._1.split("-")(0);
		val labelsRow = result._1.split("-")(1);
		val bicString = result._1.split("-")(2);
		val lines = origLines.sortBy((line) => {
			line.split(" -----").apply(0).split(" ").length
		})(Ordering.Int.reverse)
		
		val firstResults = lines.map((line) => {
		
			val data = bicString.split("#").map(y => {
				val size = y.split(",").size;
				y.split(",").zipWithIndex.map(s => {
					s._1.toDouble
				})
			});

			var bicResults = new Tuple2("","");
			var labelsUsed = "";
			
			val rowNums = line.split(" -----").apply(0).split(" ").map(_.toInt).toSeq;
			val rowsSize = rowNums.size;
			val colNames = labelsRow.split(",");
			val colsSize = colNames.size;

			val bicCols = line.split(" -----").apply(1).split(" ").filter(_.length() > 0);
			// Create new DF with rows from this bicluster
		
			var labelsDifferent = 0;
			var labelsSame = 0;
			var labelsSameLabels =  mutable.ListBuffer[String]();
		
			val rowsLength = rowNums.size;
			var topBottomRows = Math.ceil(bicValidation * rowsLength).toInt;
			if (topBottomRows < 3) {
				topBottomRows = 3;
			}

			// start from here and replicate like Driver.scala
			bicCols.zipWithIndex.foreach(col => {
				val colName = col._1;
				val idx = col._2;
				val colSorted = data.sortWith(_(idx+1) < _(idx+1));
				val firstRowLabel = colSorted(0)(colsSize-1).toInt;
				var areLabelsDifferent = false;
			
				breakable {
					for (k <- 0 to (topBottomRows-1)) {
						labelsUsed= labelsUsed + colSorted(k)(0).toInt.toString() + " ";
						labelsUsed = labelsUsed + colSorted(rowsSize-1-k)(0).toInt.toString() + " ";
						
						if (colSorted(k)(colsSize-1).toInt != firstRowLabel || 
						colSorted(rowsSize-1-k)(colsSize-1).toInt != firstRowLabel) {
							areLabelsDifferent = true;
							break;
						}
					}
				}
			
				if (areLabelsDifferent) {
					println("LABELS ARE DIFFERENT!!")
					labelsDifferent = labelsDifferent + 1;
				} else {
					println("LABELS ARE SAME!!!");
					labelsSame = labelsSame + 1;
					labelsSameLabels += String.valueOf(firstRowLabel);
				}
				println(" ");
			});

			println("Labels Same: " + labelsSame + "    Labels Different: " + labelsDifferent);
			println(" ");
		
			var allLabelsSame = false;
			if (labelsSameLabels.toList.exists(_ != labelsSameLabels.toList.head)) {
				allLabelsSame = false;
			} else {
				allLabelsSame = true;
			}
			if (labelsSame > labelsDifferent && allLabelsSame) {
				bicResults = (line.split(" -----").apply(0), labelsSameLabels.toList.head);
			}

			(bicResults, labelsUsed)
		});
    
    	val finalResults = firstResults.filter(_._1._1.trim().size > 0);
    	finalResults
  	});
  
  
	val predictedResultsCombined = predictedResultsRDD
	.collect().flatten.foreach( y => {
		y._1._1.trim().split(" ").filter(_.size > 0).map(_.toInt).toSeq.foreach(rowId => {
			val priorVal = mutablePredictedMap.getOrElse(rowId, "");
			if (priorVal != "0" && priorVal != "1" && y._1._2 != priorVal && priorVal != "") {
				println("updating: " + rowId.toString() + " new val: " + y._1._2 + " prior: " + priorVal);
				mutablePredictedMap.update(rowId, y._1._2);
			}
		});
		
		y._2.trim().split(" ").foreach(labelUsed => {
			trainingLabelsSet.add(labelUsed.toInt);
		});
	});
	
	println("Start time: " + startTime);
	println("End time: " + Calendar.getInstance().getTime());
	val endTime = Calendar.getInstance().getTime();
        
  
	val updatedRDDMap = df.rdd.map{
		case Row(rowNum: Int, label: Int, predicted: String) => {
			Row(rowNum, label, String.valueOf(mutablePredictedMap.get(rowNum).get))
		}
	}
  
  	println("updatedRDDMap done");
  
  	val finalDf = sparkSession.createDataFrame(updatedRDDMap, org.apache.spark.sql.types.StructType(MainData.getSchemaPredicted()));

	finalDf.show();
	
	val predictionAndLabels = finalDf.select("label", "predicted").map(row => {
		var prediction = row.getAs[String](1);
		if (prediction == "?") prediction = "99.0";
		else if (prediction == "") prediction = "100.0";

		val doublePrediction = prediction.toDouble;

		(doublePrediction, row(0).asInstanceOf[Int].toDouble)
	}).rdd;
  
	// represents "?" and "" (empty string) which are not there in dataset but predictions may contain those characters. 
	val additionalLabels: RDD[(Double, Double)] = sparkSession.sparkContext.parallelize(Seq((99.0, 99.0), (100.0, 100.0)));
	  
	val metrics = new MulticlassMetrics(predictionAndLabels.++(additionalLabels));
	println("Confusion matrix:");
	println(metrics.confusionMatrix);
  
	var labelsPositive = 0;
	var labelsNegative = 0;
	trainingLabelsSet.foreach(row => {
		val label = dfLabelHashMap.getOrElse(row, 9999)
		if (label == 0) {
			labelsNegative = labelsNegative + 1;
		} else if (label == 1) {
			labelsPositive = labelsPositive + 1;
		} else {
			println("no label found")
		}
	});
  
	finalDf.coalesce(1).write.mode(SaveMode.Overwrite).csv(output2FileLocation);
	val pw = new PrintWriter(new File(confusionMatrixLocation2));
	pw.write(metrics.labels.map(_.toString).mkString(","));
	pw.write("\r\n");
	pw.write("\r\n");
	pw.write("------------------------------------------- \r\n");
	pw.write(metrics.confusionMatrix.toString);
	pw.write("\r\n");
	pw.write("------------------------------------------- \r\n");
	pw.write("\r\n\n");
	pw.write("LP: ");
	pw.write(labelsPositive.toString());
	pw.write("\r\n");
	pw.write("LN: ");
	pw.write(labelsNegative.toString());
	pw.write("\r\n");
	pw.write("StartTime: ");
	pw.write(startTime.toString());
	pw.write("\r\n");
	pw.write("EndTime: ");
	pw.write(endTime.toString());
	pw.close();

	val pw1 = new PrintWriter(new File(trainingLabelsLocation2));
	pw1.write(trainingLabelsSet.mkString("\n"));
	pw1.close();
	println("Done");

//-----------------------------------------------------------------------------------

}