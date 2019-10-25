package com.amar.research;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable;
import com.amar.research.{ItemsMapTrans};
import scala.collection._;

import com.amar.research.utils.Context;
import com.amar.research.Utils.{ getMean, getRound, getTRange, getVariance};

object Process extends App with Context {
  
  // Configuration
  val minSupport = 10
  val minSupportCol = 10;
	val numPartitions = 1

  // Read Data
	val origData = sparkSession.sparkContext.textFile("src/resources/training.txt").cache();

	origData.take(5).map(println);
	//-----------------------------------------------------------------------------------
	// Preprocessing
	// takes each line of comma-separated data (last col is label)
	// converts to -> value$col ...%row
	// 0,15,15,15,0,5 -> 0$0 0$4 15$1 15$2 15$3%3
	val prepData = origData.zipWithIndex.map{ case (line, idx) => 
	  line.split(',')
//	  .dropRight(1) // label col
	  .zipWithIndex
	  .sortBy{ case(value, index) => value.toDouble}
	  .map{ case(value, index) => value + "$" + index }
	  .mkString(" ")
	  .concat("%" + idx)
	}
	
   prepData.take(5).map(println);
	//-----------------------------------------------------------------------------------
	// BicPhaseOne
	// Two-hop projection
	// Converts to -> mean_range    variance$col#value,col#value,col#value$row
	// Input: 0$0 0$4 15$1 15$2 15$3%3
	// Output: 15.0:15.33	   0.0$1#15.0,2#15.0,3#15.0$3 <-- the Bic_Str
   
	val trange1 = Utils.getTRange(0.00, 1, 0.1, 0.025)
	val trange2 = getTRange(1,10,1, 0.25)
	val trange3 = getTRange(10,50,5,1)
	val trange4 = getTRange(50, 700, 25, 5)
	val trange = List.concat(trange1, trange2, trange3, trange4)
	
	// Print trange
	val trangeList = trange.map(x => x._1 + ":" + x._2)
	println(trangeList.mkString(","));	
	
	val biclusteredData = prepData.flatMap( line => 
	    processLine(line, trange)
  )
  
  biclusteredData.take(5).map(println);
//	biclusteredData.groupBy(_.split("\t")(0)).map(println);
	
	// Gather the biclusters which fall in the same mean range
	// Store in Map -> (mean_range, List(Bic_Str))
	val reducedData = biclusteredData.groupBy(_.split("\t")(0))
	  .map( x => (x._1, x._2.toList.map(_.split("\t")(1))))

	// --------------------------------------------------------------------
	// CHARM Phase
	// algorithm finds closed frequent itemsets
	// 
//	val minsup = sparkSession.sparkContext.broadcast(minSupport)

	val afterCharm = reducedData.map(formatItemSets);
	
	val groupAfterCharm = afterCharm.flatMap(x => x).distinct().groupByKey();
  
	println("---------------------------------------------------------------------------------------------------------------------------");
	val sortedConcepts = groupAfterCharm.sortBy(_._2.size, false, numPartitions).sortBy(_._1.split(" ").length, false, numPartitions);
	val filteredConcepts = sortedConcepts
	
	// For itemsets with the same transactions, group by transaction numbers (rows)
	// Then assign the maximum length itemset to the transaction (maxBy(_.length))
  val finalConcepts = filteredConcepts.flatMap( x=> { 
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
	
	// Pick the largest biclusters
	// Load the rows and columns data, including labels
	// Sort the data by one col at a time
	// Sample the head and tail and check if the same labels
	// If same labels, then can assume same data for all the remaining rows
	// If not same labels, split the sorted rows into two sets using variou strategies
	// Strategey 1 - Entropy based split

	biclusteredData.saveAsTextFile("src/resources/output/processed");
	reducedData.saveAsTextFile("src/resources/output/reduced");
	afterCharm.saveAsTextFile("src/resources/output/charm");
	groupAfterCharm.saveAsTextFile("src/resources/output/groupCharm");
	finalConcepts.coalesce(1).saveAsTextFile("src/resources/output/final");
	
	println("---------------------------------------------------------------------------------------------------------------------------");
	println("Thread is now sleeping...");
//	Thread.sleep(100000);
	println("DONE")

	
	
	// FUNCTIONS -------------------------------------------------------------------------------------------------------------------------
	
  // Method to prepare the data and execute Charm algorithm
	def formatItemSets(data: (String, List[String])) = {
	  
	  val item_trans_map = ItemsMapTrans();
//	  $17#19.22,2#26.44,18#31.44$  row 0
	  data._2.foreach(item_trans => {
	    val subStrings = item_trans.split('$');
	    val columns = subStrings(1).split(',');
	    val row = subStrings(2).toInt
	    val trans = new mutable.TreeSet[Int]();
	    
	    columns.map(x => {
	      val mapping = x.split("#")
	      val item_set = new ItemSet()+=mapping(0);
	      val trans = item_trans_map.imt.getOrElse(item_set, new mutable.TreeSet[Int]())
	      trans.add(row);
	      item_trans_map.imt = item_trans_map.imt+( item_set -> trans)
	    })
	  });
	  
//	  println("Item Trans Map Size: " + item_trans_map.imt.size);
	  
//    println(item_trans_map.imt.toString());
//    item_trans_map.imt.take(5).toString.map(print);
    
    val b_minsup = minSupport
    val concepts = Charm(item_trans_map, b_minsup)
//    println("Concepts Size: " + concepts.imt.size);
    var concepts_str = new mutable.ListBuffer[Tuple2[String, String]]
    
    // add all cols and rows as a concept instead of adding individually
			  for(concept <- concepts.imt) {
				  val itemset = concept._1.isets
					val trans = concept._2
					
					concepts_str += Tuple2(trans.mkString(" "), itemset.mkString(" "))
			  }
    concepts_str.toList
  }
  
	
	def processLine(line: String, rangeStr: List[Tuple2[Double,Double]]): Array[String] = {
	  val rowNum = line.split('%')(1);
	  val lineArray = line.split('%')(0).split(' ');
	  val length: Int = lineArray.length;
	  var results: Array[String] = new Array[String](length);
	  val mean: Double = 0.00;
	  val variance: Double = 0.00;
	  
	  for (index <- 0 to length - 2) {
	    if (index + 2 < length) {
        val triplets: Seq[String] = Seq(lineArray(index), lineArray(index + 1), lineArray(index + 2));
        val values: Seq[Double] = triplets.map(x => x.split('$')(0).toDouble);
        val cols: Seq[String] = triplets.map(x => x.split('$')(1));
        
        val outStr = cols(0) + "#" + values(0).toString() + "," + 
          cols(1) + "#" + values(1).toString() + "," +
          cols(2) + "#" + values(2).toString()
          
        val mean: Double = getMean(values);
        val variance: Double = getVariance(values, mean);
        val varRound = getRound(variance, 7);
        val finalStr = varRound.toString() + "$" + outStr + "$" + rowNum;
        //-------------------------------------------------------------
        val meanRound = getRound(mean, 2);
        val covariance = Math.sqrt(variance)/mean;
        
        var meanRange = "";
        rangeStr.foreach(x => {
          if (x._1 <= meanRound && meanRound <= x._2) {
            meanRange = x._1.toString() + ":" + x._2.toString();
          }
        })
//        if (covariance <= 0.10) { }
        results(index) = meanRange + "\t" + finalStr;
      }
	  }
	  return results.filter(p => {
	    var result = false;
	    if (p != null) {
	      result = true;
	    }
	    result
	  });
	}
}