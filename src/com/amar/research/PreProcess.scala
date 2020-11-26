
package com.amar.research

//import com.amar.research.Utils;

object PreProcess {

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

						val mean: Double = Utils.getMean(values);
						val variance: Double = Utils.getVariance(values, mean);
						val varRound = Utils.getRound(variance, 7);
						val finalStr = varRound.toString() + "$" + outStr + "$" + rowNum;
						//-------------------------------------------------------------
						val meanRound = Utils.getRound(mean, 2);
						val covariance = Math.sqrt(variance)/mean;

						var meanRange = "";
						rangeStr.foreach(x => {
							if (x._1 <= meanRound && meanRound <= x._2) {
								meanRange = x._1.toString() + ":" + x._2.toString();
							}
						})
						        if (covariance <= 0.2) { 
						          
						          results(index) = meanRange + "\t" + finalStr;
						        }
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