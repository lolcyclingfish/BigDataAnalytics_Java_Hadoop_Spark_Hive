package com.javamakeuse.hadoop.poc.Homework2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.MultipleInputs;

//import com.javamakeuse.hadoop.poc.Homework2.Exercise1_draft.Map;




public class Exercise1 extends Configured implements Tool {

    public static class MapOne extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	//private final static DoubleWritable one = new DoubleWritable(1);
	//private Text year = new Text();

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String tokens[] = line.split("\\s+");//read in google data and split line by whitespace
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
	}
	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {		
	}
    }

    public static class MapTwo extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    	//private final static DoubleWritable one = new DoubleWritable(1);
    	//private Text year = new Text();

    	public void configure(JobConf job) {
    	}

    	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
    	}

    	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
    	    String line = value.toString();
    	    String tokens[] = line.split("\\s+");//read in google data and split line by whitespace
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
	JobConf conf = new JobConf(getConf(), Exercise1.class);
	conf.setJobName("HW2.Exercise1");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	//conf.setMapperClass(Map.class);
	conf.setJarByClass(Exercise1.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	//FileInputFormat.setInputPaths(conf, new Path(args[0]));
	MultipleInputs.addInputPath(conf,new Path(args[0]),TextInputFormat.class,MapOne.class);
	MultipleInputs.addInputPath(conf,new Path(args[1]),TextInputFormat.class,MapTwo.class);

	FileOutputFormat.setOutputPath(conf, new Path(args[2]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
	System.exit(res);
    }
}



