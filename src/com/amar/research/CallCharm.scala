package com.amar.research
import scala.collection.mutable;

object CallCharm {
    // Method to prepare the data and execute Charm algorithm
  
  var minSupport: Int = 0;
  def setMinSupport(sup: Int) {
    minSupport = sup;
  }
  
	def formatItemSets(data: (String, List[String])) = {
	  
	  val item_trans_map = ItemsMapTrans();
//	  $17#19.22,2#26.44,18#31.44$  row 0
	  data._2.foreach(item_trans => {
	    val subStrings = item_trans.split('$');
	    val columns = subStrings(1).split(',');
	    val col = subStrings(2);
	    val trans = new mutable.TreeSet[Int]();
	    
	    columns.map(x => {
	      val mapping = x.split("#")
	      // add col to itemset
	      val row = mapping(0).toInt;
	      val item_set = new ItemSet()+=col;
	      // find if this col already exists in TreeMap
	      // If exists, add the row to the list of rows already found to have similar values in this col
	      // If does not exist, create new TreeSet[Int] for this col and add to the TreeMap (imt)
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
}