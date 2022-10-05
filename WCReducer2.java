package com.mapreducetest.wc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
public class WCReducer2 extends Reducer <Text, FloatWritable, Text, FloatWritable> {
	public Float max = 0f;
	public String max_name = "";
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
	for (FloatWritable val : values) {
		if(val.get() > max)
		{
			max=val.get();
			max_name=key.toString();
		}
	}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
	      context.write(new Text(max_name), new FloatWritable(max));
	  }
}
