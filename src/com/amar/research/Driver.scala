package com.amar.research;

import com.amar.research.Process2;
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
import org.apache.spark.rdd.RDD;
import java.io._;
import scala.util.control.Breaks._;


import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance};

object Process extends App with Context {
  import sparkSession.implicits._;
  // Configuration
  val minSupport = 25; // For Charm
  val minSupportCol = 1; // For filtering concepts
	val numPartitions = 1;
	val bicValidation = 0.025; // Check 5% of rows from top and bottom for labels
	val inputFileLocation1 = "src/resources/bank-data/bank-data-normalized-ceilinged-0to-4.csv";
	val inputFileLocation2 = "src/resources/bank-data/bank-data-normalized-ceilinged-0to-4-transposed.csv";
//	val inputFileLocation = "src/resources/glass-data/glass-data-normalized.csv";
	val outputFileLocation  = inputFileLocation1.substring(0, inputFileLocation1.length()-40) + "output";
// T-Range Generation for mean-ranges for glass-dataset
//	val trange = getTRange(0.00, 80, 0.1, 0.025);
	val trange = getTRange(0.0, 4.0, 0.9, 0.03);

  // Read Original Data
	val origData = sparkSession.sparkContext.textFile(inputFileLocation1);
	origData.take(5).map(println);
	
	// Read Transposed Data
	val transposedData = sparkSession.sparkContext.textFile(inputFileLocation2);
	transposedData.take(5).map(println);
	
	// Transpose data in spark instead of reading from two files.

//val byColumnAndRow = origData.zipWithIndex.flatMap {
//  case (row, rowIndex) => row.split(',').dropRight(1).zipWithIndex.map {
//    case (number, columnIndex) => columnIndex -> (rowIndex, number)
//  }
//}
//// Build up the transposed matrix. Group and sort by column index first.
//val byColumn = byColumnAndRow.groupByKey.sortByKey().values
//// Then sort by row index.
//val transposed = byColumn.map {
//  indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
//}
//
//transposed.take(5).map(println);
	
	
	//-----------------------------------------------------------------------------------
	// 1. Preprocessing
	// takes each line of comma-separated data (last col is label)
	// converts to -> value$col ...%row
	// 0,15,15,15,0,5 -> 0$0 0$4 15$1 15$2 15$3%3
	val prepData = transposedData.zipWithIndex.map{ case (line, idx) => 
	  line.split(',')
	  // .dropRight(1) // label col
	  .zipWithIndex
	  .sortBy{ case(value, index) => value.toDouble}
	  .map{ case(value, index) => value + "$" + index }
	  .mkString(" ")
	  .concat("%" + idx)
	}
   prepData.take(5).map(println);
   

  // T-Range Generation for mean-ranges for test-dataset
//	val trange1 = getTRange(0.00, 1, 0.1, 0.025)
//	val trange2 = getTRange(1,10,1, 0.25)
//	val trange3 = getTRange(10,50,5,1)
//	val trange4 = getTRange(50, 700, 25, 5)
//	val trange = List.concat(trange1, trange2, trange3, trange4);	
	
	// Print trange
	val trangeList = trange.map(x => x._1 + ":" + x._2)
	println(trangeList.mkString(","));	
	
	//-----------------------------------------------------------------------------------
	// 2. BicPhaseOne
	// Two-hop projection
	// Converts to -> mean_range    variance$col#value,col#value,col#value$row
	// Input: 0$0 0$4 15$1 15$2 15$3%3
	// Output: 15.0:15.33	   0.0$1#15.0,2#15.1,3#15.4$3 <-- the Bic_Str
	val biclusteredData = prepData.flatMap( line => 
	    PreProcess.processLine(line, trange)
  )
  println("biclustered");
  biclusteredData.take(5).map(println);
	
	// Gather the biclusters which fall in the same mean range
	// Store in Map -> (mean_range, List(Bic_Str))
	val reducedData = biclusteredData.groupBy(_.split("\t")(0))
	  .map( x => (x._1, x._2.toList.map(_.split("\t")(1))))

	println("reduced");
	reducedData.take(5).map(println);
	// --------------------------------------------------------------------
	// 3. CHARM Phase
	// algorithm finds closed frequent itemsets

	CallCharm.setMinSupport(minSupport);
	val afterCharm = reducedData.map(CallCharm.formatItemSets);
	println("after charm");
	println(afterCharm.collect().size);
	afterCharm.take(5).map(println);
		println("---------------------------------------------------------------------------------------------------------------------------");
	
	
//	val level1 = afterCharm.flatMap(x=>x)
//	println("level 1");
//	println(level1.collect().size);
//	level1.take(5).map(println);	
//	println("---------------------------------------------------------------------------------------------------------------------------");
//	
//	
//	val level2 = level1.distinct();
//	println("level 2");
//	println(level2.collect().size);
//	level2.take(5).map(println);
//		println("---------------------------------------------------------------------------------------------------------------------------");
//	
//		
//	val level3 = level2.groupByKey();
//	println("level 3");
//	println(level3.collect().size);
//	level3.take(5).map(println);
//		println("---------------------------------------------------------------------------------------------------------------------------");
	
	
	val groupAfterCharm = afterCharm.flatMap(x => x).distinct();
  println("group after charm");
	println(groupAfterCharm.collect().size);
	groupAfterCharm.take(10).map(println);
	println("---------------------------------------------------------------------------------------------------------------------------");
	
	val filteredConcepts = groupAfterCharm
	.filter( _._2.size >= minSupportCol)
	.filter( _._1.split(" ").size >= minSupport)
//	.filter(_._2.head.equals("0"))
	.sortBy(_._1.split(" ").length, true, numPartitions) // sort such that lowest number of cols comes first
	.sortBy(_._2.size, true, numPartitions) // sort such that lowest number of rows comes first
	// sorting is done in such a way to process the smallest bics first for better accuracy. 
	// If bigger bics are processed first, they will either produce incorrect predictions or they would result in no prediction
	
	println("filtered concepts");
	filteredConcepts.take(10).map(println);
	println("---------------------------------------------------------------------------------------------------------------------------");
	
  
//  println("done");
//  Thread.sleep(100000);
  // Find all rows present in the filtered concepts
	val conceptRows: ListBuffer[Int] = new ListBuffer[Int]();
	filteredConcepts.foreach(x => {
	  x._1.split(" ").map(rowStr => {
	    conceptRows+=(rowStr.toInt)
	  });
	})
	
	// Remove duplicate rows from concept rows so the set of validation rows is created
	val uniqueRows = conceptRows.distinct;
	
	// Select the above "uniqueRows" from the orig dataset for validation purposes
	val validationRows = origData.zipWithIndex()
	.filter{ case (line, idx) => 
	  if (uniqueRows.contains(idx)) {
	    true;
	  } else {
	    false;
	  }
	}
	
	val allRows = origData.zipWithIndex()
	
	println("VALIDATION ROWS");
	println(" ");
	validationRows.take(5).map((x) => println(x));

	// Convert validation rows to DataFrame for easier manipulation
	val fileToDf = validationRows.map{ case(x, y) => BankData.mapToDF(x, y)};
	val origFileToDf = allRows.map{ case(x, y) => BankData.mapToDF(x, y)};
	
	val schema = BankData.getBankSchema();
	// GlassData.getGlassSchema();
	
	val df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));
	val origDf = sparkSession.createDataFrame(origFileToDf, org.apache.spark.sql.types.StructType(schema));
	
	val origDf_predicted = origDf.withColumn("predicted", lit(""));
	
	var predictedDfMap = origDf_predicted.rdd.map{
    case Row(rowNum: Int, col0: Double, col1: Double, col2: Double, col3: Double, label: Int, predicted: String) => (rowNum, predicted)
  }.collectAsMap();
  
  var dfLabelHashMap = origDf_predicted.rdd.map{
    case Row(rowNum: Int, col0: Double, col1: Double, col2: Double, col3: Double, label: Int, predicted: String) => (rowNum, label)
  }.collectAsMap();
  
  val mutablePredictedMap = mutable.Map(predictedDfMap.toSeq: _*);
	var trainingLabelsSet = mutable.Set[String]();
  
	var csvCount = 1;
	var sameCsvCount = 1;
	filteredConcepts.collect().foreach(x => {
	  println(" ");
	  println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
	  val rowNums = x._1.split(" ").map(_.toInt).toSeq;
	  val colNamesStr = "rowId " + x._2 + " label";
	  val colNames = colNamesStr.split(" ");
	  val bicCols = colNames.drop(1).dropRight(1);
	  // Create new DF with rows from this bicluster
	  val rowsDf = df.where(col("rowId").isin(rowNums:_*));	  
	  // Keep only columns which are part of this bicluster in the DF, rowId, and label
	  var colDf = rowsDf.select(colNames.head, colNames.tail:_*);
	  
	  var labelsSame = 0;
	  var labelsDifferent = 0;
	  var labelsSameLabels =  mutable.ListBuffer[String]();
	  
	  var topBottomRows = Math.ceil(bicValidation * rowNums.size).toInt;
	  if (topBottomRows < 2) {
	    topBottomRows = 2;
	  }

	  // Sort the data by one col at a time
	  // Sample the head and tail and check if the same labels
	  // Keep track of how many cols result in same label vs different label
	  // If labelsSame >= labelsDifferent, then predict all rows to have same label
	  // Else predict as "?" and devise strategy to split the bicluster (for instance entropy based split)
	  bicCols.foreach(colName => {
	    val colSortedDF = colDf.orderBy(asc(colName));
	    val withId = colSortedDF.withColumn("_id", monotonically_increasing_id()).orderBy("_id");
	    var k = 0;
	    val firstRowLabel = withId.head(1).apply(0).getAs[Integer]("label").toString();
	    var areLabelsDifferent = false;
	    val bottomWithId = withId.orderBy(desc("_id"));
	    breakable {
  	    for (k <- 0 to (topBottomRows-1)) {
            println(withId.head(k+1).apply(k).mkString(" "));
  	        println(bottomWithId.head(k+1).apply(k).mkString(" "));
            println("Top " + (k+1) + " row label: " + withId.head(k+1).apply(k).getAs[String]("label") + " when sorting by column: " + colName);
            println("Bottom " + (k+1) + " row label: " + bottomWithId.head(k+1).apply(k).getAs[String]("label") + " when sorting by column: " + colName);
            trainingLabelsSet.add(withId.head(k+1).apply(k).getAs[String]("rowId"));
            trainingLabelsSet.add(bottomWithId.head(k+1).apply(k).getAs[String]("rowId"));
            if ( withId.head(k+1).apply(k).getAs[Integer]("label").toString() != firstRowLabel || 
              bottomWithId.head(k+1).apply(k).getAs[Integer]("label").toString() != firstRowLabel) {
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
//  	  val firstRow = withId.head(1).apply(0);
//  	  val secondRow = withId.head(2).apply(1);
//      val lastRow = withId.orderBy(desc("_id")).head(1).apply(0);
//      val secondLastRow = withId.orderBy(desc("_id")).head(2).apply(1);
//  	  println(firstRow.mkString(" "));
//  	  println(secondRow.mkString(" "));
//  	  println(lastRow.mkString(" "));
//  	  println(secondLastRow.mkString(" "));
//  	  println("First row label: " + firstRow.getAs[String]("label") + " when sorting by column: " + colName);
//  	  println("Second row label: " + secondRow.getAs[String]("label") + " when sorting by column: " + colName);
//  	  println("Last row label: " + lastRow.getAs[String]("label") + " when sorting by column: " + colName);
//  	  println("Second Last row label: " + secondLastRow.getAs[String]("label") + " when sorting by column: " + colName);
  	  
//  	  trainingLabelsSet.add(firstRow.getAs[String]("rowId"));
//  	  trainingLabelsSet.add(secondRow.getAs[String]("rowId"));
//  	  trainingLabelsSet.add(lastRow.getAs[String]("rowId"));
//  	  trainingLabelsSet.add(secondLastRow.getAs[String]("rowId"));
//  	  
//	    if (firstRow.getAs[String]("label") != lastRow.getAs[String]("label") || 
//	        firstRow.getAs[String]("label") != secondRow.getAs[String]("label") || 
//	        firstRow.getAs[String]("label") != secondLastRow.getAs[String]("label")) {
//	      println("LABELS ARE DIFFERENT!!")
//	      labelsDifferent = labelsDifferent + 1;
//  	  } else {
//  	    println("LABELS ARE SAME!!!");
//  	    labelsSame = labelsSame + 1;
//  	    labelsSameLabels += String.valueOf(firstRow.getAs[String]("label"));
//  	  }
//      println(" ");
	  })
	  
	  println("------------------------------------------");
	  println("Final Tally: ");
	  println( "labels same: " + labelsSame);
	  println( "labels different: " + labelsDifferent);
	  println("------------------------------------------");
	  
	  var allLabelsSame = false;
	  if (labelsSameLabels.toList.exists(_ != labelsSameLabels.toList.head)) {
	    allLabelsSame = false;
	  } else {
	    allLabelsSame = true;
	  }
	  
	  if (labelsSame > labelsDifferent && allLabelsSame) {
//	    colDf = colDf.withColumn("predicted", lit(colDf.head().getAs[String]("label")));
//	    consolidated_df = consolidated_df.withColumn("predicted",
//	        when(consolidated_df.col("rowId").isin(rowNums:_*), lit(colDf.head().getAs[String]("label")))
//	        .otherwise(consolidated_df.col("predicted")));
	    
	    rowNums.foreach(rowId => {
          mutablePredictedMap.update(rowId, labelsSameLabels.toList.head);
      });
	    
	    // append labels as update instead of replacing to keep track of different predictions for same row
	    
//	    rowNums.foreach(rowId => {
//	      consolidated_df = consolidated_df.withColumn("predicted", 
//	        when(consolidated_df.col("rowId").equalTo(rowId), lit( consolidated_df.where(col("rowId").equalTo(rowId)).select("predicted").collectAsList().get(0).getString(0) + "," 
//	            + colDf.head().getAs[String]("label")))
//	        .otherwise(consolidated_df.col("predicted")))
//	    })
        var new_colDf = colDf;
        val columnNames = Seq("rowId","label","0","1","2","3")
        new_colDf = new_colDf.select( new_colDf.columns.intersect(columnNames).map(x=>col(x)): _* );
  	    println( "saving to same csv folder - " + sameCsvCount);
        
        new_colDf.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save("src/resources/bank-data/same-csv/" + sameCsvCount);
        sameCsvCount = sameCsvCount+1;
	  } else {
//	    colDf = colDf.withColumn("predicted", lit("?"));
//	    consolidated_df = consolidated_df.withColumn("predicted", 
//	        when(consolidated_df.col("rowId").isin(rowNums:_*), lit("?"))
//	        .otherwise(consolidated_df.col("predicted")))
	        
      rowNums.foreach(rowId => {
          mutablePredictedMap.update(rowId, "?");
      });
	    
	    // append labels as update insteda of replacing to keep track of different predictions for same row
       
	    // save biclusters in text file for trimax algorithm
	    // potentially call the trimax algofrom here -- sys.process stringToProcess() 
	    // https://stackoverflow.com/questions/38813810/how-to-execute-system-commands-in-scala
      var new_colDf = colDf;
      val columnNames = Seq("rowId","label","0","1","2","3")
      new_colDf = new_colDf.select( new_colDf.columns.intersect(columnNames).map(x=>col(x)): _* );
	    println( "saving to csv folder - " + csvCount);
      
      new_colDf.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save("src/resources/bank-data/csv/" + csvCount);
      csvCount = csvCount+1;
	  }
	  colDf.show();
    x
	})

//	consolidated_df.show(25);
	
	val updatedRDDMap = origDf_predicted.rdd.map{
    case Row(rowNum: Int, col0: Double, col1: Double, col2: Double, col3: Double, label: Int, predicted: String) => {
      Row(rowNum, col0, col1, col2, col3, label, String.valueOf(mutablePredictedMap.get(rowNum).get))
    }
  }
  
  val finalDf = sparkSession.createDataFrame(updatedRDDMap, org.apache.spark.sql.types.StructType(BankData.getBankSchemaPredicted()));

  finalDf.show();	
	
	val predictionAndLabels = finalDf.select("label","predicted").map( row => {
	    var prediction = row.getAs[String](1);
	    if(prediction == "?") prediction="99.0";
	    if (prediction == "") prediction = "100.00";
	    val doublePrediction = prediction.toDouble
	    
	    (doublePrediction, row(0).asInstanceOf[Int].toDouble)
	  }).rdd
	
  val additionalLabels: RDD[(Double, Double)] = sparkSession.sparkContext.parallelize(Seq((99.0, 99.0), (100.0, 100.0)))

	val metrics = new MulticlassMetrics(predictionAndLabels.++(additionalLabels));

	println("Confusion matrix:")
  println(metrics.confusionMatrix);
	
  var labelsPositive = 0;
  var labelsNegative = 0;
  val labelsString = trainingLabelsSet.mkString(",");
  val trainingLabelsInt = labelsString.split(",").map(_.toInt);
  
  trainingLabelsInt.foreach(row => {
    val label = dfLabelHashMap.getOrElse(row, 9999)
    if (label == 0) {
      labelsNegative = labelsNegative + 1;
    } else if (label == 1) {
      labelsPositive = labelsPositive + 1;
    } else {
      println("no label found")
    }
  });

  
	finalDf.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputFileLocation);  
  val pw = new PrintWriter(new File(outputFileLocation + "/confusion-matrix.txt" ))
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
  
  val pw1 = new PrintWriter(new File(outputFileLocation + "/training-labels.txt" ));
  pw1.write(trainingLabelsSet.mkString("\n"));
  pw1.close();
	// Pick the largest biclusters - Done
	// Load the rows and columns data, including labels - Done
	// Sort the data by one col at a time - Done
	// Sample the head and tail and check if the same labels - Done
	// If same labels, then can assume same data for all the remaining rows - Done
	// If not same labels, split the sorted rows into two sets using various strategies....
	// Strategy 1 - Entropy based split....

//	biclusteredData.saveAsTextFile("src/resources/output/processed");
//	reducedData.saveAsTextFile("src/resources/output/reduced");
//	afterCharm.saveAsTextFile("src/resources/output/charm");
//	groupAfterCharm.saveAsTextFile("src/resources/output/groupCharm");
	
	// For itemsets with the same transactions, group by transaction numbers (rows)
	// Then assign the maximum length itemset to the transaction (maxBy(_.length))
	// finalConcepts are formatted as strings to print to file
	// This variable is not used in validation
  val finalConcepts = filteredConcepts.map( x => { 
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
    val string = rows.mkString(" ") + " , " + cols.mkString(" ") + " % " + rows.size + " , " + cols.size;
    string
  });
//	finalConcepts.map(println);
	// save finalConcepts to file
	finalConcepts.coalesce(1).saveAsTextFile(outputFileLocation + "/concepts/");
	
	println("---------------------------------------------------------------------------------------------------------------------------");
	println("Thread is now sleeping...");
//	Thread.sleep(100000); // Used to keep spark server running for debugging
	println("DONE")
}