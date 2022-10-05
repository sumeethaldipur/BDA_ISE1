package com.mapreducetest.wc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
public class WCMapper1 extends Mapper <LongWritable, Text, Text, FloatWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException,
	InterruptedException {
	try {
		// Skip the header in case of csv
				if(key.get() == 0) {
					return;
				}
	String[] line = value.toString().split(",");
	String publisher = line[5];
	Float total_shipped;
	try {
		total_shipped = Float.parseFloat(line[9]);
	}
	catch (NumberFormatException e){
	    total_shipped = 0f;
	}
	context.write(new Text (publisher),new FloatWritable(total_shipped));
	}catch(ArrayIndexOutOfBoundsException e){
	}
	}
}
