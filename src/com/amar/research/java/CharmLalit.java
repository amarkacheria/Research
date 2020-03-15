package com.amar.research.java;

/********************************************************************************************
Author : Lalit Singh
Date : Fe 02, 2017
Work : This code processes the entire input from hdfs and provide final output from reducer

 *******************************************************************************************/
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.*;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CharmLalit {

    //defining custom data structure for keeping rows while processing
    class ItemSet implements Comparable<ItemSet> {
		Set<String> itemSet;

		public ItemSet() {
			itemSet = new TreeSet<>();
		}

		public void add(String item) {
			itemSet.add(item);
		}

		public void addAll(ItemSet itemSet2) {
			itemSet.addAll(itemSet2.itemSet);
		}

		public boolean contains(ItemSet itemSet2) {
			return itemSet.containsAll(itemSet2.itemSet);
		}

	    @Override
	    public int compareTo(ItemSet o) {
			return itemSet.toString().compareTo(o.toString());
	    }

	    @Override
	    public String toString() {
			return itemSet.toString();
	    }
    }

    //defining skipset for skipping the pruned branched while processing
    private Set<ItemSet> skipSet;
    
    private static int minSupport;

    //defining instance of the main function
    public CharmLalit() {
		this.skipSet = new TreeSet<>();
    }

    /**
     * Function to extract the items and transactions from an input .txt file.
     * format the entire file in the format in which it will be processed
     * Format: <String item, Set<Integer> transactions>
     *
     * @param ip
     * @param file
     */


    //function to read input record in the format used by Charm Algorithm
    private void formatInputNew(Map<ItemSet, Set<Integer>> ip, String line) {
		String[] tline = line.split("\t");
		String meanRange = tline[0];
		String[] inval = tline[1].split(";");
		for(String s: inval) {
			String[] tstr = s.split("\\$");
			//String[] columns = tstr[1].split(",");
			//String row = tstr[2];
			String[] columns = tstr[1].split(",");
			String row = tstr[2];
			
			for(String col : columns) {
				ItemSet key = new ItemSet();
				//key.add(col.split("#")[0]);
				key.add(col);
				Set<Integer> value = ip.getOrDefault(key, new TreeSet());
				value.add(Integer.parseInt(row));
				ip.put(key, value);
			}
		}	
    }
    
    /**
     * Function to replace Xi with X.
     *
     * @param curr
     * @param target
     * @param map
     */

    private void replaceInItems(ItemSet curr, ItemSet target, Map<ItemSet, Set<Integer>> map) {
		List<ItemSet> temp = new ArrayList<>();
		// Identify the items to be replaced.
		for (ItemSet key : map.keySet()) {
			if (key.contains(curr)) {
				temp.add(key);
			}
		}
		// Update each item
		for (ItemSet key : temp) {
			Set<Integer> val = map.get(key);
			map.remove(key);
			key.addAll(target);
			map.put(key, val);
		}
    }

    /**
     * Incorporating the 4 charm properties.
     *
     * @param xi
     * @param xj
     * @param y
     * @param minSup
     * @param nodes
     * @param newN
     * @return xi
     */
    private ItemSet charmProp(ItemSet xi, ItemSet xj, Set<Integer> y, int minSup, Map<ItemSet, Set<Integer>> nodes,
		Map<ItemSet, Set<Integer>> newN) {
		if (y.size() >= minSup) {
			// temp = xi U xj
			ItemSet temp = new ItemSet();
			temp.addAll(xi);
			temp.addAll(xj);
	    	if (nodes.get(xi).equals(nodes.get(xj))) { // Property 1
				skipSet.add(xj);
				replaceInItems(xi, temp, newN);
				replaceInItems(xi, temp, nodes);
				return temp;
	    	} else if (nodes.getOrDefault(xj, new TreeSet<>()).containsAll(nodes.getOrDefault(xi, new TreeSet<>()))) {
				 // Property 2
				replaceInItems(xi, temp, newN);
				replaceInItems(xi, temp, nodes);
				return temp;
	    	} else if (nodes.getOrDefault(xi, new TreeSet<>()).containsAll(nodes.getOrDefault(xj, new TreeSet<>()))) {
				// Property 3
				skipSet.add(xj);
				newN.put(temp, y);
	    	} else {
				if (!nodes.getOrDefault(xi, new TreeSet<>()).equals(nodes.getOrDefault(xj, new TreeSet<>()))) {
					// Property 4
		    		newN.put(temp, y);
				}
	    	}
		}
		return xi;
    }

    /**
     * Function to check if an item set is subsumed by the existing item sets.
     *
     * @param c
     * @param y
     * @return true/false
     */
    private boolean isSubsumed(Map<ItemSet, Set<Integer>> c, Set<Integer> y) {
		for (Set<Integer> val : c.values()) {
			if (val.equals(y)) {
				return true;
			}
		}
		return false;
    }

    /**
     * Charm - Extended routine
     *
     * @param nodes
     * @param c
     * @param minSup
     */
    private void charmExtended(Map<ItemSet, Set<Integer>> nodes, Map<ItemSet, Set<Integer>> c, int minSup) {
		List<ItemSet> items = new ArrayList(nodes.keySet());
		for (int idx1 = 0; idx1 < items.size(); idx1++) {
			ItemSet xi = items.get(idx1);
			if (skipSet.contains(xi)) continue;
			ItemSet x_prev = xi;
			Set<Integer> y;
			ItemSet x = null;
			Map<ItemSet, Set<Integer>> newN = new TreeMap<>();
			for (int idx2 = idx1 + 1; idx2 < items.size(); idx2++) {
				ItemSet xj = items.get(idx2);
				if (skipSet.contains(xj)) continue;
				// x = xi U xj
				x = new ItemSet();
				x.addAll(xi);
				x.addAll(xj);
				y = nodes.getOrDefault(xi, new TreeSet<>());
				Set<Integer> temp = new TreeSet<>();
				temp.addAll(y);
				temp.retainAll(nodes.getOrDefault(xj, new TreeSet<>()));
				xi = charmProp(xi, xj, temp, minSup, nodes, newN);
			}
			if (!newN.isEmpty()) {
				charmExtended(newN, c, minSup);
			}
			if (x_prev != null && nodes.get(x_prev) != null && !isSubsumed(c, nodes.get(x_prev))) {
				c.put(x_prev, nodes.get(x_prev));
			}
			if (x != null && nodes.get(x) != null && !isSubsumed(c, nodes.get(x))) {
				c.put(x, nodes.get(x));
			}
		}
    }


    /**
     * CHARM routine - Items are ordered lexicographically.
     *
     * @param ip
     * @param minSup
     * @return c
     */
    public Map<ItemSet, Set<Integer>> charm(Map<ItemSet, Set<Integer>> ip, int minSup) {
		// Eliminate items that don't satisfy the min support property
		Map<ItemSet, Set<Integer>> filteredIp = new HashMap();
		Set<ItemSet> keys = ip.keySet();
		for (ItemSet key : keys) {
			Set<Integer> value = ip.get(key);
			if (value.size() >= minSup) {
				filteredIp.put(key, value);
			}
		}
		// Generate the closed item sets
		Map<ItemSet, Set<Integer>> c = new TreeMap<>();
		charmExtended(filteredIp, c, minSup);
		return c;
    }
    
    public static class CharmImplement extends Mapper<LongWritable, Text, Text, Text> {
        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
		private Text outKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			CharmLalit obj = new CharmLalit();
			Map<ItemSet, Set<Integer>> ip = new TreeMap<>();
			//Map<String, List<String>> originalData = new HashMap<>();
			//defining minimum support
			int minsup = minSupport;

			//reading input record
			//originalData = obj.formatInput(ip, line);
			obj.formatInputNew(ip, line);

			//calling charm algorithm
			Map<ItemSet, Set<Integer>> c = obj.charm(ip, minsup);

			//C have all the closed itemset and cvCalculayion function will be called
			//in turn which call function getCoVar() and the output to reducer is written
			//in the function cvCalculation()

			Set finalSet = c.entrySet();
			Iterator it = finalSet.iterator();
			while (it.hasNext()) {
				Map.Entry citem = (Map.Entry)it.next();
				String[] outkey = citem.getKey().toString().replace("[","").replace("]","").replace(" ","").split(",");
				String[] outval = citem.getValue().toString().replace("[","").replace("]","").replace(" ","").split(",");

				if(outkey.length>=2) {
					String foutval="";
					for(String s : outval) {
						foutval = foutval+" "+s;
					}
					foutval = foutval.trim();

					String foutkey="";
					for(String s: outkey) {
						foutkey = foutkey+" "+s;
					}
					foutkey = foutkey.trim();
					outKey.set(foutval);
					word.set(foutkey);
					context.write(outKey,word);
				}
			}
		}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text word = new Text();
		private Text outKey = new Text();
	
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
			String outstr="";
			int cntr=0;
			for (Text val : values) {
				//outstr = outstr+";"+val.toString();
				outstr = val.toString();
				cntr++;
			}
			//outstr = outstr.substring(1)+"\t"+cntr;
			//outstr = outstr+"\t"+cntr;
			word.set(outstr);
			context.write(key,word);
		}
    }

   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        minSupport = Integer.parseInt(args[2]);
        Job job = new Job(conf, "CharmV5");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setJarByClass(Charm.class);

        job.setMapperClass(CharmImplement.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
			//job.setReducerClass(Reduce.class);
		//to increasing the number of reducer uncomment the line below
		job.setNumReduceTasks(5);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

	
}

