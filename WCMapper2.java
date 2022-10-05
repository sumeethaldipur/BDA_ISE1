package com.mapreducetest.wc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
public class WCMapper2 extends Mapper <LongWritable, Text, Text, FloatWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException,
	InterruptedException {
	try {
	String[] line = value.toString().split("\t");
//	String[] state_and_type = line[0].split(",");
//	String state = state_and_type[0];
//	String type = state_and_type[1];
//	String count = line[1];
//	Text k = new Text(state);
//	Text v = new Text(type + "," + count);
	context.write(new Text(line[0]),new FloatWritable(Float.parseFloat(line[1])));
	}catch(ArrayIndexOutOfBoundsException e){
	}
	}
}
