package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	//private final static DoubleWritable one = new DoubleWritable(1);
	//private Text year = new Text();

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String tokens[] = line.split(",");//read in music data and split line by comma	    
	    if(tokens[164].matches("[0-9]+")) {
	    	String keyString = tokens[1]+","+tokens[2]+","+tokens[3];
		    double valueYear = Double.parseDouble(tokens[165]);
		    output.collect(new Text(keyString), new DoubleWritable(valueYear));	
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
	    double year = 0;
	    while (values.hasNext()) {
	    	DoubleWritable temp = values.next();
	    	if (temp.get() <= 2010 && temp.get() >= 2000) {
	    	year = temp.get();	
	    	}
	    }   
	    output.collect(key, new DoubleWritable(year));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise3.class);
	conf.setJobName("HW2.Exercise3");

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
	int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
	System.exit(res);
    }
}



