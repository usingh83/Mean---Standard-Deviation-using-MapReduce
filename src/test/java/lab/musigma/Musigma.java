package lab.musigma;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Musigma {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				context.write(new Text("count"), new Text(value.toString()+","+Long.parseLong(value.toString())*Long.parseLong(value.toString())+",1"));
		}
	}
	public static class Combine extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			long sum = 0; 
			BigInteger sumos=new BigInteger("0");
			long count=0;
			for(Text line:values){
				String[] res=line.toString().split(",");
				sum+=Long.parseLong(res[0]);
				sumos=sumos.add(new BigInteger(res[1]));
				count+=Integer.parseInt(res[2]);
			}
			String val=sum+","+sumos+","+count;
			context.write(key,new Text(val));
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			long sum = 0; 
			BigInteger sumos=new BigInteger("0");
			long count=0;
			for(Text line:values){
				String[] res=line.toString().split(",");
				sum+=Long.parseLong(res[0]);
				sumos=sumos.add(new BigInteger(res[1]));
				count+=Integer.parseInt(res[2]);
			}
			double mu=(double)sum/(double)count;
			BigInteger variance=sumos.divide(BigInteger.valueOf(count)).subtract(BigInteger.valueOf((long) (mu*mu)));
			String val="mean = "+mu+"  variance = "+variance.doubleValue();
			result.set(val);
			context.write(new Text("Result : "),result);
		}
	}
  	public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  	// get all args
  	if (otherArgs.length != 2) {
  	System.err.println("Usage: WordCount <in> <out>");
  	System.exit(2);
  	}
  	Job job = new Job(conf, "WordCount");
  	job.setJarByClass(Musigma.class);
  	job.setMapperClass(Map.class);
  	job.setCombinerClass(Combine.class);;
  	job.setReducerClass(Reduce.class);
  	job.setOutputKeyClass(Text.class);
  	job.setOutputValueClass(Text.class);
  	//set the HDFS path of the input data
  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	// set the HDFS path for the output
  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  	//Wait till job completion
  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
  }
