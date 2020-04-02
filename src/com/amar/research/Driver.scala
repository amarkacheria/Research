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

import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance};

object Process extends App with Context {
  
  // Configuration
  val minSupport = 3 // For Charm
  val minSupportCol = 3; // For filtering concepts
	val numPartitions = 1
//	val inputFileLocation = "src/resources/test-data/test-dataset-labels-merged.csv";
	val inputFileLocation = "src/resources/glass-data/glass-data.csv";
	val outputFileLocation = inputFileLocation.substring(0, inputFileLocation.length()-4) + "-output";
	val isRowIdPresent = true;

  // Read Data
	val origData = sparkSession.sparkContext.textFile(inputFileLocation);
	origData.take(5).map(println);
	
	//-----------------------------------------------------------------------------------
	// 1. Preprocessing
	// takes each line of comma-separated data (last col is label)
	// converts to -> value$col ...%row
	// 0,15,15,15,0,5 -> 0$0 0$4 15$1 15$2 15$3%3
	val prepData = origData.zipWithIndex.map{ case (line, idx) => 
	  line.split(',')
	  .drop(1) // row id
	  .dropRight(1) // label col
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
	
 // T-Range Generation for mean-ranges for glass-dataset
//	val trange = getTRange(0.00, 80, 0.1, 0.025);
	val trange = getTRange(-5, 5, 0.333333, 0.2);
	
	
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
  
  biclusteredData.take(5).map(println);
	
	// Gather the biclusters which fall in the same mean range
	// Store in Map -> (mean_range, List(Bic_Str))
	val reducedData = biclusteredData.groupBy(_.split("\t")(0))
	  .map( x => (x._1, x._2.toList.map(_.split("\t")(1))))

	// --------------------------------------------------------------------
	// 3. CHARM Phase
	// algorithm finds closed frequent itemsets

	CallCharm.setMinSupport(minSupport);
	val afterCharm = reducedData.map(CallCharm.formatItemSets);
	
	afterCharm.take(5).map(println);
	
	val groupAfterCharm = afterCharm.flatMap(x => x).distinct().groupByKey();
  
	groupAfterCharm.take(5).map(println);
	
	println("---------------------------------------------------------------------------------------------------------------------------");

//	groupAfterCharm.collect().map(x => {
//	  println(x);
//	  println(x._1);
//	  println(x._2.foreach(print(_)));
//	  x._2.map( x=> {print(x); print(" ")}) 
////	  x._2.filter(c => c.split(" ").size > minSupportCol)
//	  println("---------------------------------------------------------------");
//	});
	
	val filteredConcepts = groupAfterCharm
	.filter( _._2.filter(c => c.split(" ").size > minSupportCol).size > 0)
	.sortBy(_._1.split(" ").length, false, numPartitions).sortBy(_._2.size, false, numPartitions);
	
	filteredConcepts.take(10).map(println);
  
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
	
	val allRows = origData.zipWithIndex();
	
	println("VALIDATION ROWS");
	println(" ");
	validationRows.take(5).map((x) => println(x));

	// Convert validation rows to DataFrame for easier manipulation
	val fileToDf = validationRows.map{ case(x, y) => GlassData.mapToDF(x, y)};
	val origFileToDf = allRows.map{ case(x, y) => GlassData.mapToDF(x, y)};
	
	val schema = GlassData.getGlassSchema();
	
	val df = sparkSession.createDataFrame(fileToDf, org.apache.spark.sql.types.StructType(schema));
	val origDf = sparkSession.createDataFrame(origFileToDf, org.apache.spark.sql.types.StructType(schema));
	var consolidated_df = origDf.withColumn("predicted", lit(""));
	
	df.show(2);
	filteredConcepts.collect().foreach(x => {
	  println(" ");
	  println("xxxxxxxxxxxxxxx------------------------------------------------------------------");
	  val rowNums = x._1.split(" ").map(_.toInt).toSeq;
	  val colNamesStr = "rowId " + x._2.maxBy(_.split(" ").size) + " label";
	  val colNames = colNamesStr.split(" ");
	  val bicCols = colNames.drop(1).dropRight(1);
	  // Create new DF with rows from this bicluster
	  val rowsDf = df.where(col("rowId").isin(rowNums:_*));	  
	  // Keep only columns which are part of this bicluster in the DF, rowId, and label
	  var colDf = rowsDf.select(colNames.head, colNames.tail:_*);
	  
	  colDf.show();
	  var labelsSame = 0;
	  var labelsDifferent = 0;

	  // Sort the data by one col at a time
	  // Sample the head and tail and check if the same labels
	  // Keep track of how many cols result in same label vs different label
	  // If labelsSame >= labelsDifferent, then predict all rows to have same label
	  // Else predict as "?" and devise strategy to split the bicluster (for instance entropy based split)
	  bicCols.foreach(colName => {
	    val colSortedDF = colDf.orderBy(asc(colName));
	    val withId = colSortedDF.withColumn("_id", monotonically_increasing_id()).orderBy("_id");
	    withId.show();
  	  val firstRow = withId.head(1).apply(0);
      val lastRow = withId.orderBy(desc("_id")).head(1).apply(0);
  	  println(firstRow.mkString(" "));
  	  println(lastRow.mkString(" "));
  	  println("First row label: " + firstRow.getAs[String]("label") + " when sorting by column: " + colName);
  	  println("Last row label: " + lastRow.getAs[String]("label") + " when sorting by column: " + colName);
  	  
	    if (firstRow.getAs[String]("label") != lastRow.getAs[String]("label")) {
	      println("LABELS ARE DIFFERENT!!")
	      labelsDifferent = labelsDifferent + 1;
  	  } else {
  	    println("LABELS ARE SAME!!!");
  	    labelsSame = labelsSame + 1;
  	  }
	  })
	  
	  println(" ");
	  println("Final Tally: ");
	  println( "labels same: " + labelsSame);
	  println( "labels different: " + labelsDifferent);
	  
	  if (labelsSame >= labelsDifferent) {
	    colDf = colDf.withColumn("predicted", lit(colDf.head().getAs[String]("label")));
	    consolidated_df = consolidated_df.withColumn("predicted",
	        when(consolidated_df.col("rowId").isin(rowNums:_*), lit(colDf.head().getAs[String]("label")))
	        .otherwise(consolidated_df.col("predicted")));
	  } else {
	    colDf = colDf.withColumn("predicted", lit("?"));
	    consolidated_df = consolidated_df.withColumn("predicted", 
	        when(consolidated_df.col("rowId").isin(rowNums:_*), lit("?"))
	        .otherwise(consolidated_df.col("predicted")))
	  }
	    	  
  	colDf.show();
    x
	})

	consolidated_df.show(25);
	consolidated_df.coalesce(1).write.csv(outputFileLocation);
	
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
  val finalConcepts = filteredConcepts.flatMap( x => { 
    val n_trans = x._1.split(" ").size
    val supportConcepts = x._2.filter(c => c.split(" ").size > minSupportCol);
    val tup: Array[String] = new Array[String](supportConcepts.size);
    supportConcepts.zipWithIndex.map(c => {
        val n_items = c._1.split(" ").size
        tup(c._2) = x._1+","+c._1++" % "+n_trans+","+n_items
    });
    //     val n_dup = x._2.size
    //     val tup = x._1+","+x._2.maxBy(_.length)+" % "+n_trans+","+n_items+","+n_dup
     tup
  });
	
	// save finalConcepts to file
	finalConcepts.coalesce(1).saveAsTextFile("src/resources/output/final");
	
	println("---------------------------------------------------------------------------------------------------------------------------");
	println("Thread is now sleeping...");
//	Thread.sleep(100000); // Used to keep spark server running for debugging
	println("DONE")
}