package Task2_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2_2_Reducer1 extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double sum = 0;
		for (Text value : values) {
			sum += Double.parseDouble(value.toString());
		}
		context.write(new Text("Loss"), new Text(String.valueOf(sum)));
	}
}