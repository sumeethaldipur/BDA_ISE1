package com.mapreducetest.wc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
public class WCReducer1 extends Reducer <Text, FloatWritable, Text, FloatWritable> {
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
	FloatWritable result = new FloatWritable();
	int sum = 0;
	for (FloatWritable val : values) {
	sum += val.get();
	} result.set(sum);
	context.write(new Text(key.toString()), result);
	}
}
