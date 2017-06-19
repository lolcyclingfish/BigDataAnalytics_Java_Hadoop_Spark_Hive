package com.javamakeuse.hadoop.poc.Homework1;

import java.io.*;
import java.util.*;
//import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import com.google.common.collect.Lists;

public class Exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
//https://sunilmistri.wordpress.com/2015/02/13/mapreduce-example-for-minimum-and-maximum-value-by-group-key/
    	//DoubleWritable is the MinMaxDuration in the example above
	//private DoubleWritable fourthField = new DoubleWritable(); //will be the value to be reduced;
	private Text groupByVars = new Text(); //word will be the key, which is the four fields for group-by concatenated  
    
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    
		String line = value.toString();
		String tokens[] = line.split(",");//read in csv data and split by comma
		
		String filter = tokens[tokens.length-1];
		
		if(filter.equals("false")) {
			double targetCol = Double.parseDouble(tokens[3]);
			String groupBy = tokens[29] + "," + tokens[30]+"," + tokens[31]+ "," + tokens[32];
            //input into mapper
			output.collect(new Text(groupBy), new DoubleWritable(targetCol));
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

	//https://www.tikalk.com/java/WordCountAverage/
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    //need to find mean values through iterator values	
		double sum = 0;
		double meanValue=0;
		//get size/length of a iterator: http://stackoverflow.com/questions/9720195/what-is-the-best-way-to-get-the-count-length-size-of-an-iterator
		//int count = Lists.newArrayList(values).size();
		double count = 0;
	    while (values.hasNext()) {
	    	//if (int k=0; k< ibmFields.length; k++){
	    		
	    	//} //to select where ibm[44] is true
	  		sum += values.next().get();
	  		count++;
	    }
	    meanValue = sum/count;
	    output.collect(key, new DoubleWritable(meanValue));//input into reduced result
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise2.class);
	conf.setJobName("Exercise2");

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
	int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
	System.exit(res);
    }
}

