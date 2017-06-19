package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise2_draft extends Configured implements Tool {

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
	    	String filterYear = tokens[1]; //in 1-grams file, 2nd element of line is year
	    	if(filterYear.matches("[0-9]+")) {
	    		double valueVolume = Double.parseDouble(tokens[3]);
	    		output.collect(new Text("genericKey"), new DoubleWritable(valueVolume));
	    	} 
	    } else if (tokens.length==5) {
	    	String filterYear = tokens[2]; //in 2-grams file, 3rd element of line is year
	    	if(filterYear.matches("[0-9]+")) {
	    		double valueVolume = Double.parseDouble(tokens[4]);
	    		output.collect(new Text("genericKey"), new DoubleWritable(valueVolume));
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
	    double sumSquare= 0;
	    double stdDev = 0;
	    while (values.hasNext()) {
			sum += values.next().get();
			count++;
	    }
	    meanValue = sum/count;
	    while (values.hasNext()) {
	    	sumSquare += (values.next().get() - meanValue)*(values.next().get() - meanValue);
	    	count++;
	    }
	    stdDev = Math.sqrt(sumSquare/count);
	    output.collect(key, new DoubleWritable(stdDev));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise2_draft.class);
	conf.setJobName("HW2.Exercise2_draft");

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
	int res = ToolRunner.run(new Configuration(), new Exercise2_draft(), args);
	System.exit(res);
    }
}


