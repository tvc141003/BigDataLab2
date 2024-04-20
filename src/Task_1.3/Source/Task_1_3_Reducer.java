package Task_1_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task_1_3_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	private FloatWritable result = new FloatWritable(0);
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		for (FloatWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
