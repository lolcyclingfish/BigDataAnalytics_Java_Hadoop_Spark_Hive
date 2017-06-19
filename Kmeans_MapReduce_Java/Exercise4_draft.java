package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise4_draft extends Configured implements Tool {
	// mapper class
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {



		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String tokens[] = line.split(",");// read in music data and split buy,
			String keyArtists = tokens[2].trim();
			double valueDuration = Double.parseDouble(tokens[3]);
			output.collect(new Text(keyArtists), new DoubleWritable(valueDuration));
		}


	}

	// reducer class
	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {


		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
				Reporter reporter) throws IOException {
			// need to partition the key and assign parts to 5 reducers
			// http://stackoverflow.com/questions/17134341/hadoop-mapreduce-sort-reduce-output-using-the-key
			// http://stackoverflow.com/questions/14320313/mapreduce-sort-values
			// http://stackoverflow.com/questions/16434526/sort-an-iterator-of-strings

			double maxValue = Double.MIN_VALUE;
			while (values.hasNext()) {
				double next = values.next().get();
				if (next > maxValue) {
					maxValue = next;
				}
			}
			// need to sort key by alphabetical orders
			output.collect(key, new DoubleWritable(maxValue));
		}
	}

	// Partitioner class
	// https://www.tutorialspoint.com/map_reduce/map_reduce_partitioner.htm
	// http://dailyhadoopsoup.blogspot.com/2014/01/partitioning-in-mapreduce.html
	// https://github.com/roanjain/hadoop-partitioner/blob/master/src/com/mapred/partitioner/PartitionerDemo.java
	public static class Partition extends MapReduceBase implements Partitioner<Text, DoubleWritable> {

		public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

			String artist = key.toString();
			String firstletter = artist.substring(0, 1).toLowerCase();

			if (firstletter.compareTo("d") < 0) {
				return 0;
			} else if (firstletter.compareTo("h") < 0) {
				return 1;
			} else if (firstletter.compareTo("l") < 0) {
				return 2;
			} else if (firstletter.compareTo("q") < 0) {
				return 3;
			} else {
				return 4;
			}		
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise4_draft.class);
		conf.setJobName("HW2.Exercise4");

		conf.setNumReduceTasks(5);

		// conf.setBoolean("mapred.output.compress", true);
		// conf.setBoolean("mapred.compress.map.output", true);

		// Set output format for key-value pair
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		// set mapper, reducer class
		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(Partition.class);
		conf.setReducerClass(Reduce.class);

		// set input and output format for data
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// set output and input path for data
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise4_draft(), args);
		System.exit(res);
	}
}





