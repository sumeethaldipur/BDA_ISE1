package com.mapreducetest.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WCDriver {
public static void main(String[] args) throws Exception {
Configuration conf1 = new Configuration();
Job job1 = Job.getInstance(conf1, "word count");
job1.setJarByClass(WCDriver.class);
job1.setMapperClass(WCMapper1.class);
job1.setCombinerClass(WCReducer1.class);
job1.setReducerClass(WCReducer1.class);
job1.setOutputKeyClass(Text.class);
job1.setOutputValueClass(FloatWritable.class);
FileInputFormat.addInputPath(job1, new Path(args[0]));
FileOutputFormat.setOutputPath(job1, new Path(args[1]));
job1.waitForCompletion(true);
Configuration conf2 = new Configuration();
Job job2 = Job.getInstance(conf2, "job2");
job2.setJarByClass(WCDriver.class);
job2.setMapperClass(WCMapper2.class);
job2.setCombinerClass(WCReducer2.class);
job2.setReducerClass(WCReducer2.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(FloatWritable.class);
FileInputFormat.addInputPath(job2, new Path(args[1]));
FileOutputFormat.setOutputPath(job2, new Path(args[2]));
System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}