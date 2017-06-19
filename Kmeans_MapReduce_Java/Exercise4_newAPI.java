package com.javamakeuse.hadoop.poc.Homework2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class Exercise4_newAPI extends Configured implements Tool {
	/**
	 */
	public static class PartitionerMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			try{
				String line = value.toString();
			    String tokens[] = line.split(",");//read in music data and split line by comma
			    String keyArtists = tokens[2];
			    double valueDuration = Double.parseDouble(tokens[3]);
			    context.write(new Text(keyArtists),new DoubleWritable(valueDuration));
			}
			catch(Exception e){
				System.err.println(e);
			}
		}
	}
	/**
	 * Each partition processed by different reducer tasks 
	 */
	public static class PartitionerReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values,Context context) throws IOException, InterruptedException {
			double maxValue = Double.MIN_VALUE;
		    while(values.hasNext()) {
		    	double next = values.next().get();
		        if(next > maxValue) {
		            maxValue = next;
		        }    
		    }
			context.write(new Text(key),new DoubleWritable(maxValue));
		}
	}
	
	/**
 
	 */
	public static class MyPartitioner extends Partitioner<Text,DoubleWritable>{
		public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {    		
			/*
			String line = value.toString();
	   	    String tokens[] = line.split(",");
	   	    */
	        //String firstLetter = tokens[2].substring(0,1).toLowerCase(); 
	        String firstLetter = key.toString().substring(0,1).toLowerCase();
			if(firstLetter=="a"||firstLetter=="b"||firstLetter=="c"||firstLetter=="d"){
				return 0;
				} else if(firstLetter=="e"||firstLetter=="f"||firstLetter=="g"||firstLetter=="h") {
					return 1;
					} else if (firstLetter=="i"||firstLetter=="j"||firstLetter=="k"||firstLetter=="l") {
						return 2;
						} else if (firstLetter=="m"||firstLetter=="n"||firstLetter=="o"||firstLetter=="p") {
							return 3;
							} else {
								return 4;
							}
					
			/**
			String line = value.toString();
		    String tokens[] = line.split(",");//read in music data and split line by comma
		    String Artist = tokens[2];
			
			if (numReduceTasks==0) {
        		return 0;
        	}
        	if (Artist.toString().substring(0,1).toLowerCase().matches("[a-dA-D]")) {
        		return 1 % numReduceTasks;
        	} else if (Artist.toString().substring(0,1).toLowerCase().matches("[e-hE-H]")) {
        		return 2 % numReduceTasks;
        	} else if (Artist.toString().substring(0,1).toLowerCase().matches("[i-lI-L]")) {
        		return 3 % numReduceTasks;
        	} else if (Artist.toString().substring(0,1).matches("[m-zM-Z]")) {
        		return 4 % numReduceTasks;
        	} else {
        		return 5 % numReduceTasks;
        	}
        	**/
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res= ToolRunner.run(new Configuration(),new Exercise4_newAPI(),args);
		System.exit(res);
	}
	public int run(String[] args) throws Exception {
		
		/**
		if(args.length!=2){
			System.out.print("Run as --> hadoop jar /path/to/name.jar /inputdataset /output");
			System.exit(-1);
		}
		**/
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Exercise4_newAPI.class);
		
		//Set number of reducer tasks
		job.setNumReduceTasks(5);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(PartitionerMap.class);
		job.setReducerClass(PartitionerReduce.class);

		//Set Partitioner Class
		job.setPartitionerClass(MyPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
