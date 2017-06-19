package com.javamakeuse.hadoop.poc.Homework1;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise1 extends Configured implements Tool {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private IntWritable temp = new IntWritable();
	    private Text year = new Text();

	    public void configure(JobConf job) {
	    }

	    protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	    }

	    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
	    	//http://stackoverflow.com/questions/9780619/java-getting-a-string-from-an-arraylist
	    	//String[] arrayOfLines = line.split("\\s+"); //no need to split cause mapreduce read in each row as a line by default
	    	String listofYear = line.substring(15,19);
	    	year.set(listofYear);
	    
	    	int listofTemp = Integer.parseInt(line.substring(87,92));
	    	//String listofQuality = line.substring(92,93);
	    
	    	if(listofTemp !=9999) {
	    		temp.set(listofTemp);
	    		output.collect(year, temp);
	    	}	
	}


	protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}
}

public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
		//find the max value in <IntWritable> values, and return it in output
		//http://stackoverflow.com/questions/25080207/shortcut-to-determine-maximum-element-in-iteratorintwritable-in-reduce-met
		int maxValue = Integer.MIN_VALUE;
	    while(values.hasNext()) {
	    	int next = values.next().get();
	        if(next > maxValue) {
	            maxValue = next;
	        }
	    }

		
	    output.collect(key, new IntWritable(maxValue));
	}

	protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}
	
}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise1.class);
	conf.setJobName("Exercise1");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
	System.exit(res);
    }
}
