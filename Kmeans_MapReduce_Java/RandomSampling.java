package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RandomSampling extends Configured implements Tool {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    	
    private Text mykey=new Text();
    private Text myvalue=new Text();
	private double sample_percentage;	
	
	public void configure(JobConf job) {
		sample_percentage = Double.parseDouble(job.get("sample_percentage"));	
	}
//mapper function    	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
            String line=value.toString();
            
		Random generator = new Random();
		int r = generator.nextInt(100);
		if (r <= (sample_percentage*100)){
    			mykey.set(line);
    			myvalue.set("");
    			context.collect(mykey,myvalue);
		}
    		
         }//mapper
    }//class 
   
   public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), RandomSampling.class);
	conf.setJobName("RandomSampling");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(Map.class);	
    conf.setNumReduceTasks(0);
   
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
		

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	conf.set("sample_percentage",args[2]);
	
	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new RandomSampling(), args);
	System.exit(res);
    }
}
