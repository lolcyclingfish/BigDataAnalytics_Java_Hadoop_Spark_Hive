package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.Map;
import org.apache.hadoop.mapred.lib.MultipleInputs;


import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Exercise2_test extends Configured implements Tool{
	public static class MapOne extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void configure(JobConf job){

		}

		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException{

		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

			String line = value.toString();
			String[] words = line.split("\\s+");
			String year = words[1];
			double nov = Double.parseDouble(words[3]);
			if (Pattern.matches("[a-zA-Z]+", year) == false && year.length() >3){

				output.collect(new Text("a"), new DoubleWritable(nov));
			}
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}
	public static class MapTwo extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void configure(JobConf job){

		}

		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException{

		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

			String line = value.toString();
			String[] words = line.split("\\s+");
			String word = words[0] + words[1];
			String year = words[2];
			double nov = Double.parseDouble(words[4]);
			if (Pattern.matches("[a-zA-Z]+", year) == false && year.length() >3){
				if (Pattern.matches("[a-zA-Z]+", year) == false && year.length() >3){
					output.collect(new Text("a"), new DoubleWritable(nov));
				}
			}
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}


	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {


		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			List<DoubleWritable> cache = new ArrayList<DoubleWritable>();
			double sum = 0;
			double cnt = 0;
			while (values.hasNext()) {
				DoubleWritable val = values.next();
				sum = sum + val.get();
				cache.add(val);
				cnt++;
			}
			double avg_m = (double)sum/cnt;
			double sumdiff = 0;
			for(DoubleWritable value:cache) {
				  sumdiff += Math.pow(value.get()-avg_m,2);
			}
			double std = Math.sqrt(sumdiff);
			output.collect(key, new DoubleWritable(std));
		}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
}
	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise2.class);
		conf.setJobName("HW2.Exercise2");

		// conf.setNumReduceTasks(0);

		// conf.setBoolean("mapred.output.compress", true);
		// conf.setBoolean("mapred.compress.map.output", true);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		//conf.setMapperClass(Map.class);
		conf.setJarByClass(Exercise2.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		MultipleInputs.addInputPath(conf,new Path(args[0]),TextInputFormat.class,MapOne.class); //argu[0] be path for 1-gram file
		MultipleInputs.addInputPath(conf,new Path(args[1]),TextInputFormat.class,MapTwo.class); //argu[1] be path for 2-gram file

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
	    }

	    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
		System.exit(res);
	    }
	}



