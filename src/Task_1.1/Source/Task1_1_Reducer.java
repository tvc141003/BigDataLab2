package Task1_1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1_1_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	private FloatWritable result = new FloatWritable(0);
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float value = 0;
		for (FloatWritable val : values) {
			value += val.get();
		}
		result.set(value);
		context.write(key, result);
	}
}
