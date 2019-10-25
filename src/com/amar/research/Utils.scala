package com.amar.research

import scala.collection.mutable.ListBuffer;

object Utils {
  
  	def getMean(seq: Seq[Double]): Double = { 
	  return seq.foldLeft((0.0, 1)) { case ((avg, idx), next) => (avg + (next - avg)/idx, idx + 1) }._1
  }

	def getVariance(seq: Seq[Double], mean: Double): Double = { 
	  return getMean(seq.map(x => Math.pow(x-mean, 2)))
  }
	
	def getRound(value: Double, decimalPlaces: Int): Double = {
	  return BigDecimal(value).setScale(decimalPlaces, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
	
	def getTRange(start: Double, end: Double, step: Double, overlap: Double = 0.00): List[Tuple2[Double, Double]] = {
	   val range = ListBuffer[Tuple2[Double, Double]]();
	   var currentPos: Double = start;
	   while ( currentPos < end ) {
	     range += Tuple2(currentPos-overlap, currentPos+step+overlap)
	     currentPos = currentPos + step;
	   }
	   return range.toList.map(x => (getRound(x._1,2), getRound(x._2,2)))
	}
}