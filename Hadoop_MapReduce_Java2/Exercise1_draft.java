package com.javamakeuse.hadoop.poc.Homework2;
//com.javamakeuse.hadoop.poc.Homework2.Exercise1

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise1_draft extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	//private final static DoubleWritable one = new DoubleWritable(1);
	//private Text year = new Text();

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String tokens[] = line.split("\\s+");//read in google data and split line by whitespace
	    if (tokens.length==4) {
	    	String filter = tokens[0]; //filter is the 1 gram word in 1st token
	    	String year = tokens[1];
	    	if (year.matches("[0-9]+")) {
		    	if(filter.toLowerCase().contains("nu")) {
		    		double valueVolume = Double.parseDouble(tokens[3]);
		    		String keyString = tokens[1]+","+"nu"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	} else if (filter.toLowerCase().contains("chi")) {
		    		double valueVolume = Double.parseDouble(tokens[3]);
		    		String keyString = tokens[1]+","+"chi"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	} else if (filter.toLowerCase().contains("haw")) {
		    		double valueVolume = Double.parseDouble(tokens[3]);
		    		String keyString = tokens[1]+","+"haw"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	}
	    	}
	    } else if (tokens.length==5) {
	    	String filter = tokens[0]+","+tokens[1];
	    	String year = tokens[2];
	    	if (year.matches("[0-9]+")) {
		    	if(filter.toLowerCase().contains("nu")) {
		    		double valueVolume = Double.parseDouble(tokens[4]);
		    		String keyString = tokens[2]+","+"nu"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	} else if (filter.toLowerCase().contains("chi")) {
		    		double valueVolume = Double.parseDouble(tokens[4]);
		    		String keyString = tokens[2]+","+"chi"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	} else if (filter.toLowerCase().contains("haw")) {
		    		double valueVolume = Double.parseDouble(tokens[4]);
		    		String keyString = tokens[2]+","+"haw"; //key set as substringmatched+year
		    		output.collect(new Text(keyString), new DoubleWritable(valueVolume));
		    	}
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
	    double sum = 0;
	    double count = 0;
	    double meanValue = 0;
	    while (values.hasNext()) {
		sum += values.next().get();
		count++;
	    }
	    meanValue = sum/count;
	    output.collect(key, new DoubleWritable(meanValue));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise1_draft.class);
	conf.setJobName("HW2.Exercise1_draft");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

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
	int res = ToolRunner.run(new Configuration(), new Exercise1_draft(), args);
	System.exit(res);
    }
}


