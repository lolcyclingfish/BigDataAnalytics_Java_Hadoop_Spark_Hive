package com.javamakeuse.hadoop.poc.Homework1;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import com.google.common.collect.Lists;

public class Test extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
//https://sunilmistri.wordpress.com/2015/02/13/mapreduce-example-for-minimum-and-maximum-value-by-group-key/
    	//FloatWritable is the MinMaxDuration in the example above
	private FloatWritable fourthField = new FloatWritable(); //one will be the value to be reduced;
	private Text groupByVars = new Text(); //word will be the key, which is the four fields for group-by concatenated  
    
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    String[] ibmFields = value.toString().split(",");
	    //read in csv data and split by comma
		//create joinedVars from string list concatenating the field 30 to 33
	    
	    
	    
	    
	    
	    StringJoiner joiner = new StringJoiner("");
        joiner.add(ibmFields[30]).add(ibmFields[31]).add(ibmFields[32]).add(ibmFields[33]);
        String joinedVars = joiner.toString();
        
        groupByVars.set(joinedVars); //assign it to key for group by operation
        fourthField.set(Float.parseFloat(ibmFields[4])); //asign it to value for group by operation
        output.collect(groupByVars, fourthField);
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	//https://www.tikalk.com/java/WordCountAverage/
	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    //need to find mean values through iterator values	
		int sum = 0;
		float meanValue=0;
		//get size/length of a iterator: http://stackoverflow.com/questions/9720195/what-is-the-best-way-to-get-the-count-length-size-of-an-iterator
		//int count = Lists.newArrayList(values).size();
		int count = 0;
	    while (values.hasNext()) {
	    	//if (int k=0; k< ibmFields.length; k++){
	    		
	    	//} //to select where ibm[44] is true
	    count++;
		sum += values.next().get();
		meanValue = sum/count;
	    }
	    output.collect(key, new FloatWritable(meanValue));
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Test.class);
	conf.setJobName("Test");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

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
	int res = ToolRunner.run(new Configuration(), new Test(), args);
	System.exit(res);
    }
}


