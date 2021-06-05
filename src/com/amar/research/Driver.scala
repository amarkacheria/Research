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
import org.apache.spark.rdd.RDD;
import java.io._;
import scala.util.control.Breaks._;
import org.apache.spark.SparkContext._;

import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance };

object Process extends App with Context {
	import sparkSession.implicits._;
	// Configuration
	val minSupport = 500; // For Charm
	val minSupportCol = 1; // For filtering concepts
	val bicValidation = 0.05; // Check 5% of rows from top and bottom for labels
	val folder = "skin-data";
	val trange = getTRange(0.0, 255.0, 5, 1);
	
//	val inputFileLocation1 = "data/" + folder + "/skin-data.csv";
//	val inputFileLocation2 = "data/" + folder + "/skin-data-transposed.csv";
//	val folderLocation = "data/" + folder;
//	val saveLocation = "./";
	
	val inputFileLocation1 = "src/resources/skin-data/skin-data-trunc.csv";
	val inputFileLocation2 = "src/resources/skin-data/skin-data-transposed-trunc.csv";
	val folderLocation = "src/resources/skin-data/output";
	val saveLocation = "src/resources/skin-data";

	// Read Original Data
	val origData = sparkSession.sparkContext.textFile(inputFileLocation1);
	println("Original");
	origData.take(5).map(println);

	// Read Transposed Data - not used if transposing data in spark
	 val transposedData = sparkSession.sparkContext.textFile(inputFileLocation2);
//	 transposedData.take(5).map(println);

	// Transpose data in spark instead of reading from two files.

//	val byColumnAndRow = origData.zipWithIndex.flatMap {
//		case (row, rowIndex) => row.split(',').dropRight(1).zipWithIndex.map {
//			case (number, columnIndex) => columnIndex -> (rowIndex, number)
//		}
//	}
//	// Build up the transposed matrix. Group and sort by column index first.
//	val byColumn = byColumnAndRow.groupByKey.sortByKey().values;
//	// Then sort by row index.
//	val transposedData = byColumn.map {
//		indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2).mkString(",")
//	};
//	println("Transposed");
//	transposedData.take(5).map(println);

	//-----------------------------------------------------------------------------------
	// 1. Preprocessing
	// takes each line of comma-separated data (last col is label)
	// converts to -> value$col ...%row
	// 0,15,15,15,0,5 -> 0$0 0$4 15$1 15$2 15$3%3
	val prepData = transposedData.zipWithIndex.map { case (line, idx) =>
		line.split(',')
		// .dropRight(1) // label col
		.zipWithIndex
		.sortBy { case (value, index) => value.toDouble }
		.map { case (value, index) => value + "$" + index }
		.mkString(" ")
		.concat("%" + idx)
	}
	println("Processed");
//	prepData.take(5).map(println);

	// T-Range Generation for mean-ranges for test-dataset
	// Used if multiple T-ranges are required
	//	val trange1 = getTRange(0.00, 1, 0.1, 0.025)
	//	val trange2 = getTRange(1,10,1, 0.25)
	//	val trange3 = getTRange(10,50,5,1)
	//	val trange4 = getTRange(50, 700, 25, 5)
	//	val trange = List.concat(trange1, trange2, trange3, trange4);

	// Print trange
//	val trangeList = trange.map(x => x._1 + ":" + x._2)
//	println(trangeList.mkString(","));

	//-----------------------------------------------------------------------------------
	// 2. BicPhaseOne
	// Two-hop projection
	// Converts to -> mean_range    variance$col#value,col#value,col#value$row
	// Input: 0$0 0$4 15$1 15$2 15$3%3
	// Output: 15.0:15.33	   0.0$1#15.0,2#15.1,3#15.4$3 <-- the Bic_Str
	val biclusteredData = prepData.flatMap(line => PreProcess.processLine(line, trange));
	println("Biclustered");
//	biclusteredData.take(5).map(println);

	// Gather the biclusters which fall in the same mean range
	// Store in Map -> (mean_range, List(Bic_Str))
	val reducedData = biclusteredData
		.groupBy(_.split("\t")(0))
		.map(x => (x._1, x._2.toList.map(_.split("\t")(1))))

	println("Reduced");
//	reducedData.take(5).map(println);
	// --------------------------------------------------------------------
	// 3. CHARM Phase
	// algorithm finds closed frequent itemsets

	CallCharm.setMinSupport(minSupport);
	val afterCharm = reducedData.map(CallCharm.formatItemSets);
	println("After Charm");
//	afterCharm.take(5).map(println);
	println("---------------------------------------------------------------------------------------------------------------------------");

	val groupAfterCharm = afterCharm.flatMap(x => x).distinct();
	println("Group After Charm");
//	groupAfterCharm.take(10).map(println);
	println("---------------------------------------------------------------------------------------------------------------------------");

	val conceptRows: ListBuffer[Int] = new ListBuffer[Int]();

	val filteredConcepts: RDD[(String, String)] = groupAfterCharm
		.filter(_._2.split(" ").length >= minSupportCol)
		.filter(_._1.split(" ").length >= minSupport);

	println("Filtered Concepts");
//	filteredConcepts.take(1).map(println);
	println("---------------------------------------------------------------------------------------------------------------------------");

	// Collect Filtered Concepts
	// 1. Can find all rows which are used in the concepts - validation rows
	// 2. This smaller list is used in the next step when top/bottom rows are checked to predict labels
	// 	  The collected concepts are tied in with the original data and then distributed for processing

	val collectedConcepts = filteredConcepts.collect();

	// Find concept rows
	collectedConcepts.foreach(x => {
		x._1.split(" ").map(rowStr => {
			conceptRows += (rowStr.toInt)
		});
	});

	// Remove duplicate rows from concept rows so the set of validation rows is created
	val uniqueRows = conceptRows.distinct;

	// Select the above "uniqueRows" from the orig dataset for validation purposes
	val validationRows = origData.zipWithIndex()
		.filter { case (line, idx) =>
			if (uniqueRows.contains(idx)) {
				true;
			} else {
				false;
			}
		}

	println("Validation Rows");
	println("---------------------------------------------------------------------------------------------------------------------------");
	validationRows.take(5).map((x) => println(x));

	// Create original DF of all rows to be used to create final list of predictions
	val allRows = origData.zipWithIndex();

	// Convert validation rows to DataFrame for easier manipulation
	val fileToDf = validationRows.map { case (x, y) => MainData.mapToDF(x, y) };
	val origFileToDf = allRows.map { case (x, y) => MainData.mapToDF(x, y) };

	val schema = MainData.getSchema();

	val df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));
	val origDf = sparkSession.createDataFrame(origFileToDf, org.apache.spark.sql.types.StructType(schema));

	// Add predicted column to original DF
	val origDf_predicted = origDf.withColumn("predicted", lit(""));

	// Create map of row -> prediction
	var predictedDFSubset = origDf_predicted.select("rowId", "predicted");
	var predictedDfMap = predictedDFSubset.rdd.map {
		case Row(rowId: Int, predicted: String) => (rowId, predicted)
	}.collectAsMap();

	// Craete map of row -> label
	var dfLabelSubset = origDf_predicted.select("rowId", "label");
	var dfLabelHashMap = dfLabelSubset.rdd.map {
		case Row(rowId: Int, label: Int) => (rowId, label)
	}.collectAsMap();

	// Mutable map which will be used to consolidate all the predictions for each row
	val mutablePredictedMap = mutable.ListMap(predictedDfMap.toSeq: _*);
	// Training labels set will be used to consolidate all the labels used during prediction
	var trainingLabelsSet = mutable.Set[String]();

	// Sort the filtered concepts in a way that the last ones being processed are expected to be the most accurate
	// In the future, need to come up with a strategy to take predictions from different concepts and then choose the best one
	val sortedConcepts = collectedConcepts
		.sortWith(_._2.split(" ").length < _._2.split(" ").length) // Smaller cols first
		.sortWith(_._1.split(" ").length > _._1.split(" ").length) // Larger rows first
	// Less rows and more cols should be the best clusters and should be processed last so they overwrite previous predictions

	// Map used to gather all the concepts and the corresponding data
	val FCDF = mutable.ListMap[String, String]();
	// To keep track of indices after sorting the array
	var count = 0;

	// Time consuming loop to take all FCs and combine with the original data
	// Chose to do this instead of appending original data through the entire Charm process
	val predictedResultsRDD = sortedConcepts.foreach(x => {
//		println(" ");
//		println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		count = count + 1;
//		println(count + "/" + collectedConcepts.size);
		val rowNums = x._1.split(" ").map(_.toInt);
		val colNamesStr = "rowId " + x._2 + " label";
		val colNames = colNamesStr.split(" ");
		// Create new DF with rows and cols from this bicluster
		// Collect this data from the distributed data for each concept
		// (expensive, but less expensive than propagating data through the whole charm process and running into "out of memory")
//		println("ROWS: " + rowNums.mkString(","));
		println(rowNums.length);
		val colDf = df.filter(col("rowId").isin(rowNums:_*)).select(colNames.head, colNames.tail: _*).rdd.collect();

		var df_string = "";
		var rows = "";

		// Append row/col data with each concept so it can be processed in parallel
		colDf.foreach((r) => {
			// Row/col data
			df_string = df_string + "#" + r.toString().drop(1).dropRight(1);
			// Keeping track of rows so it can be verified
			rows = rows + r.toString().drop(1).dropRight(1).split(",")(0) + " ";
		});

//		println("rows: " + rows.trim().split(" ").size);
//		println("cols: " + x._2.split(" ").size);
		if (rows.trim().size > 0) {
			// Add to map that will be processed in parallel
			// Key = Index - rows - cols
			// Value = Data for this bicluster's row and col data
			// 1,1.2,3.4,6.7,0#5,1.3,3.5,6.8,0#...
			// Index is used in the driver to make sure we use predictions from the "most likely to be true" biclusters (few rows, more cols)
			FCDF.update(count.toString() + "-" + rows.trim() + "-" + x._2, df_string.drop(1));
		}

	});

	// Parallelize above map so the predictions for each bic can happen in parallel
	val parallelFCDF = sparkSession.sparkContext.parallelize(FCDF.toSeq);

	// Predict results in parallel
	val predictedResultsRDD2 = parallelFCDF.map((x) => {

		import sparkSession.implicits._;

		val index = x._1.trim().split("-")(0).toInt;
		val fc_rows = x._1.trim().split("-")(1);
		val fc_cols = x._1.trim().split("-")(2);
		val rowNums = fc_rows.trim().split(" ").map(_.toInt).toSeq;
		val colNamesStr = "rowId " + fc_cols + " label";
		val colNames = colNamesStr.split(" ");
		val bicCols = colNames.drop(1).dropRight(1).filter(_.length() > 0);
		val colsSize = colNames.size;
		val rowsSize = rowNums.size;

		// Create Array[Array[Double]] from the string for easier processing
		val data = x._2.split("#").map(y => {
			val size = y.split(",").size;
			y.split(",").zipWithIndex.map(s => {
				s._1.toDouble
			})
		});

		// These are the final results from each bic processing
		// bicResults = ('fc_rows', 'prediction') => ('1,5,14,78', '1') || ('1,5,14,78', '?')
		// labelsUsed = string of rowIds which were used for prediction.
		// These are used in results to figure out how many labels were needed in total and how many predictions were made using those labels
		var bicResults = new Tuple2("", "");
		var labelsUsed = "";
		var saveArray : Array[String] = new Array(rowsSize + 1);

		// Keep track of predictions after sorting by each column
		var labelsSame = 0;
		var labelsDifferent = 0;
		var labelsSameLabels = mutable.ListBuffer[String]();

		// Find how many rows need to be validated from the top/bottom
		// Controlled by "bicValidation" parameter
		var topBottomRows = Math.ceil(bicValidation * rowNums.size).toInt;
		if (topBottomRows < 2) {
			topBottomRows = 2;
		}

		// Sort the data by one col at a time
		// Sample the top and bottom rows and check if they have the same labels
		// Keep track of how many cols result in same label vs different label
		// If labelsSame >= labelsDifferent, then predict all rows to have same label
		// Else predict as "?" and use trimax to split the bicluster
		bicCols.zipWithIndex.foreach(colName => {
			val idx = colName._2;
			val colSorted = data.sortWith(_(idx + 1) < _(idx + 1));

			val firstRowLabel = colSorted(0)(colsSize - 1).toInt;
			var areLabelsDifferent = false;
			breakable {
				for (k <- 0 to (topBottomRows - 1)) {
					//  println(colSorted(k).mkString(" "));
					//  println(colSorted(rowsSize-1-k).mkString(" "));
					//  println("Top " + (k+1) + " row label: " + colSorted(k)(colsSize-1).toInt + " when sorting by column: " + colName._1);
					//  println("Bottom " + (k+1) + " row label: " + colSorted(rowsSize-1-k)(colsSize-1).toInt + " when sorting by column: " + colName._1);

					labelsUsed = labelsUsed + colSorted(k)(0).toInt.toString() + " ";
					labelsUsed = labelsUsed + colSorted(rowsSize - 1 - k)(0).toInt.toString() + " ";
					if (colSorted(k)(colsSize - 1).toInt != firstRowLabel ||
						colSorted(rowsSize - 1 - k)(colsSize - 1).toInt != firstRowLabel) {

						areLabelsDifferent = true;
						break;
					}
				}
			}

			if (areLabelsDifferent) {
				labelsDifferent = labelsDifferent + 1;
			} else {
				labelsSame = labelsSame + 1;
				labelsSameLabels += String.valueOf(firstRowLabel);
			}
//			println(" ");
		});

//		println("------------------------------------------");
//		println("Final Tally: " + index);
//		println("labels same: " + labelsSame);
//		println("labels different: " + labelsDifferent);
//		println("------------------------------------------");

		var allLabelsSame = false;
		if (labelsSameLabels.toList.exists(_ != labelsSameLabels.toList.head)) {
			allLabelsSame = false;
		} else {
			allLabelsSame = true;
		}
		// All labels are same
		// Saving this array is not needed but it is done for verification purposes.
		// Remove this for final timing
		if (labelsSame > labelsDifferent && allLabelsSame) {
//			val saveArray: Array[String] = new Array(rowsSize + 1);
			saveArray(0) = colNamesStr.split(" ").mkString(",") + "\r\n";
			data.zipWithIndex.foreach(d => {
				val size = d._1.size;
				var dataRow = "";
				d._1.zipWithIndex.foreach(cell => {
					if (cell._2 == 0) { // First col is rowId so use Int
						dataRow = dataRow + cell._1.toInt.toString() + ",";
					} else if (cell._2 == size - 1) { // Last col is label so use Int and no comma
						dataRow = dataRow + cell._1.toInt.toString();
					} else { // Data columns
						dataRow = dataRow + cell._1.toString() + ",";
					}
				});

				saveArray(d._2 + 1) = dataRow + "\r\n";
			});
			// Create path if does not exist
//			val file1: File = new File(saveLocation + "/same-csv");
//			val result1 = file1.mkdir();
//			val file2: File = new File(saveLocation + "/same-csv/" + index);
//			val result2 = file2.mkdir();
//			val samePw = new PrintWriter(new File(saveLocation + "/same-csv/" + index + "/" + index + ".csv"));
//			saveArray.foreach(str => {
//				samePw.write(str);
//			});
//			samePw.close();
			
			// bicResults will capture the rowIds and the predicted value for those rows
			bicResults = (fc_rows, labelsSameLabels.toList.head);
		} else {
			// Labels are not same so these bics should be saved so trimax can try to split and predict
//			val saveArray: Array[String] = new Array(rowsSize + 1);
			saveArray(0) = colNamesStr.split(" ").mkString(",") + "\r\n";
			data.zipWithIndex.foreach(d => {
				val size = d._1.size;
				var dataRow = "";
				d._1.zipWithIndex.foreach(cell => {
					if (cell._2 == 0) { // First col is rowId so use Int
						dataRow = dataRow + cell._1.toInt.toString() + ",";
					} else if (cell._2 == size - 1) { // Last col is label so use Int and no comma
						dataRow = dataRow + cell._1.toInt.toString();
					} else { // Data columns
						dataRow = dataRow + cell._1.toString() + ",";
					}
				});

				saveArray(d._2 + 1) = dataRow + "\r\n";
			});
			// Create path if does not exist
//			val file1: File = new File(saveLocation + "/csv");
//			val result1 = file1.mkdir();
//			val file2: File = new File(saveLocation + "/csv/" + index);
//			val result2 = file2.mkdir();
//			val diffPw = new PrintWriter(new File(saveLocation + "/csv/" + index + "/" + index + ".csv"));
//			saveArray.foreach(str => {
//				diffPw.write(str);
//			});
//			diffPw.close();
			// Set these rows as could not be predicted
			bicResults = (fc_rows, "?");
		}

		labelsUsed = labelsUsed.trim();
		// Each bic will emit index, the results predicted, and the labels used
		(index, bicResults, labelsUsed, saveArray)
	});

	// Reducer to combine the predicted results processed in parallel
	// Sort the results based on index given when all concepts were sorted based on row/col size
	// Updating the map in the same order makes sure that most accurate predictions are used last (therby overwriting previous predictions)
	val predictedResultsCombined = predictedResultsRDD2
		.collect()
		.sortWith(_._1 < _._1)
		.foreach(y => {

			y._2._1.split(" ").map(_.toInt).toSeq.foreach(rowId => {
				mutablePredictedMap.update(rowId, y._2._2);
			});

			y._3.split(" ").foreach(labelUsed => {
				trainingLabelsSet.add(labelUsed);
			});
			
			if (y._2._2 == "?") {
				val file1: File = new File(saveLocation + "/csv");
				val result1 = file1.mkdir();
				val file2: File = new File(saveLocation + "/csv/" + y._1);
				val result2 = file2.mkdir();
				val diffPw = new PrintWriter(new File(saveLocation + "/csv/" + y._1 + "/" + y._1 + ".csv"));
				y._4.foreach(str => {
					diffPw.write(str);
				});
				diffPw.close();
			} else{
				val file1: File = new File(saveLocation + "/same-csv");
				val result1 = file1.mkdir();
				val file2: File = new File(saveLocation + "/same-csv/" + y._1);
				val result2 = file2.mkdir();
				val samePw = new PrintWriter(new File(saveLocation + "/same-csv/" + y._1 + "/" + y._1 + ".csv"));
				y._4.foreach(str => {
					samePw.write(str);
				});
				samePw.close();
				
				
			}
		});

	// Consruct Confusion Matrix
	// Create map of all rows (rowId, label, prediction)
	val updatedRDDSubset = origDf_predicted.select("rowId", "label");
	val updatedRDDMap = updatedRDDSubset.rdd.map {
		case Row(rowId: Int, label: Int) => {
			Row(rowId, label, String.valueOf(mutablePredictedMap.get(rowId).get))
		}
	}

	val finalDf = sparkSession.createDataFrame(updatedRDDMap, org.apache.spark.sql.types.StructType(MainData.getSchemaPredicted()));

	finalDf.show();

	val predictionAndLabels = finalDf.select("label", "predicted").map(row => {
		var prediction = row.getAs[String](1);
		if (prediction == "?") prediction = "99.0";
		if (prediction == "") prediction = "100.00";
		val doublePrediction = prediction.toDouble

		(doublePrediction, row(0).asInstanceOf[Int].toDouble)
	}).rdd;

	val additionalLabels: RDD[(Double, Double)] = sparkSession.sparkContext.parallelize(Seq((99.0, 99.0), (100.0, 100.0)))

	val metrics = new MulticlassMetrics(predictionAndLabels.++(additionalLabels));

	println("Confusion matrix:")
	println(metrics.confusionMatrix);

	// Find how many labels used were Positive vs Negative
	var labelsPositive = 0;
	var labelsNegative = 0;
	val labelsString = trainingLabelsSet.mkString(",");
	val trainingLabelsInt = labelsString.split(",").map(_.toInt);

	trainingLabelsInt.foreach(row => {
		val label = dfLabelHashMap.getOrElse(row, 9999);
		if (label == 0) {
			labelsNegative = labelsNegative + 1;
		} else if (label == 1) {
			labelsPositive = labelsPositive + 1;
		} else {
			println("no label found")
		}
	});

	// Write the confusion matrix results to a file
	finalDf.coalesce(1).write.mode(SaveMode.Overwrite).csv(folderLocation + "/output");
	val outputFolder: File = new File(saveLocation + "/output");
	val resultOutputFolder = outputFolder.mkdir();
	val pw = new PrintWriter(new File(saveLocation + "/output" + "/confusion-matrix.txt"))
	pw.write(metrics.labels.map(_.toString).mkString(","))
	pw.write("\r\n");
	pw.write("\r\n");
	pw.write("------------------------------------------- \r\n");
	pw.write(metrics.confusionMatrix.toString)
	pw.write("\r\n");
	pw.write("------------------------------------------- \r\n");
	pw.write("\r\n\n");
	pw.write("LP: ");
	pw.write(labelsPositive.toString());
	pw.write("\r\n");
	pw.write("LN: ");
	pw.write(labelsNegative.toString());
	pw.close();

	val pw1 = new PrintWriter(new File(saveLocation + "/output" + "/training-labels.txt"));
	pw1.write(trainingLabelsSet.mkString("\n"));
	pw1.close();

	// Print concepts to a file
	val pw2 = new PrintWriter(new File(saveLocation + "/output" + "/concepts.txt"));
	val printConcepts = sortedConcepts.foreach(x => {
		val rows = x._1.split(" ");
		val cols = x._2.split(" ");
		//    val tup: Array[String] = new Array[String](supportConcepts.size);
		//    supportConcepts.zipWithIndex.map(c => {
		//        val n_items = c._1.size
		//        tup(c._2) = x._1+","+c._1++" % "+n_trans+","+n_items
		//    });
		//     val n_dup = x._2.size
		//     val tup = x._1+","+x._2.maxBy(_.length)+" % "+n_trans+","+n_items+","+n_dup
		//     tup
		val printString = rows.size + " , " + cols.size + " % " +  cols.mkString(" ") + " , " + rows.mkString(" ");
		pw2.write("" + printString);
		pw2.write("\r\n");
	});
	
	// save printConcepts to file
	pw2.close();

	println("---------------------------------------------------------------------------------------------------------------------------");

	println("DONE");
	println("Thread is now sleeping...");
	Thread.sleep(1000); // Used to keep spark server running for debugging

}