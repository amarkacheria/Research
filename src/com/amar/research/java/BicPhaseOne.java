package com.amar.research.java;

/********************************************************************************************
Author : Lalit Singh
Date : May 07, 2016
 *******************************************************************************************/
import java.io.IOException;
import java.util.*;
import java.util.Enumeration;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BicPhaseOne {
	// function for calculating mean of the given list of values
	public static double getMean(ArrayList<Double> inData) {
		double meanVal = 0.0;
		for (int i = 0; i < inData.size(); i++) {
			meanVal = meanVal + inData.get(i);
		}
		return meanVal / inData.size();
	}

	
	// function for calculating variance of the list of input values
	public static double getVariance(ArrayList<Double> inData, double meanVal) {
		double varVal = 0.0;
		double sqrdVal = 0.0;
		for (int i = 0; i < inData.size(); i++) {
			sqrdVal = sqrdVal + ((inData.get(i) - meanVal) * (inData.get(i) - meanVal));
		}
		varVal = sqrdVal / inData.size();
		return varVal;
	}

	// function for rounding the output value
	public static double getRound(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text outKey = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] valStr = null;
			String[] inStr = null;
			if (line.length() > 1) {

				// Interval +/-2% for higgs dataset
				String tRange = "0.00:0.33,0.33:0.66,0.66:1.00,1.00:1.33,1.33:0.66,1.66:2.00,2.00:2.33,2.33:0.66,2.66:3.00,3.00:3.33,3.33:0.66,3.66:4.00,4.00:4.33,4.33:0.66,4.66:5.00,5.00:5.33,5.33:0.66,5.66:6.00,6.00:6.33,6.33:0.66,6.66:7.00,7.00:7.33,7.33:0.66,7.66:8.00,8.00:8.33,8.33:0.66,8.66:9.00,9.00:9.33,9.33:0.66,9.66:10.00,10.00:10.33,10.33:0.66,10.66:11.00,11.00:11.33,11.33:0.66,11.66:12.00,12.00:12.33,12.33:0.66,12.66:13.00,13.00:13.33,13.33:0.66,13.66:14.00,14.00:14.33,14.33:0.66,14.66:15.00,15.00:15.33,15.33:0.66,15.66:16.00";

				String[] rangeStr = tRange.split(",");
				String keyStr = "";
				inStr = line.split("\t");

				valStr = inStr[1].split(" ");
				String outStr = "";
				int cntr = 0;
				double mean = 0.0;
				double variance = 0.0;

				for (int i = 0; i < valStr.length; i++) {
					cntr = i + 2;
					
					if (cntr < valStr.length) {
						List inData = new ArrayList();
						inData.add(valStr[i]);
						inData.add(valStr[i + 1]);
						inData.add(valStr[i + 2]);

						ArrayList<Double> values = new ArrayList<Double>();
						String[] val1 = valStr[i].split("\\$");
						String[] val2 = valStr[i + 1].split("\\$");
						String[] val3 = valStr[i + 2].split("\\$");
						values.add(Double.parseDouble(val1[0]));
						values.add(Double.parseDouble(val2[0]));
						values.add(Double.parseDouble(val3[0]));
						mean = getMean(values);
						variance = getVariance(values, mean);
						// mean = getRound(mean,1);
						variance = getRound(variance, 7);

						// outStr = colNum[0]+","+colNum[1]+","+colNum[2];
						// outStr = outStr.substring(1);
						outStr = val1[1] + "#" + val1[0] + "," + val2[1] + "#" + val2[0] + "," + val3[1] + "#"
								+ val3[0];

						// keyStr = Double.toString(mean)+"$"+Double.toString(variance)+"$"+outStr;
						String str = Double.toString(variance) + "$" + outStr + "$" + inStr[0]; // + $label
						word.set(str);

						double covar = Math.sqrt(variance) / mean;
						mean = getRound(mean, 2);

						for (String s : rangeStr) {
							double rMin = Double.parseDouble(s.split(":")[0]);
							double rMax = Double.parseDouble(s.split(":")[1]);

							// modification for negative values
							if (rMin <= mean && mean <= rMax) {
								if (covar <= 0.00050) // changing coefficient of variation range to 2%
								// if(covar<=0.17) //changing coefficient of variation range to 17%
								// if(covar<=0.25) //changing coefficient of variation range to 25%
								// if(covar<=0.060) //changing coefficient of variation range to 6%
								{
									keyStr = s;
									outKey.set(keyStr);
									context.write(outKey, word);
									keyStr = "";
								}
							}
						}
						outStr = "";
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text word = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String outStr = "";
			int cntr = 0;
			int flag = 0;
			for (Text val : values) {
				outStr = outStr + ";" + val.toString();
			}

			outStr = outStr.substring(1);
			word.set(outStr);
			context.write(key, word);
			outStr = "";
			cntr = 0;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "bicPhaseOne");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(BicPhaseOne.class);

		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// job.setReducerClass(Reduce.class);
		// to increasing the number of reducer uncomment the line below
		job.setNumReduceTasks(200);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
