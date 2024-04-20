package Task2_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2_2_Reducer3 extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String cluster = key.toString().split("_")[1];
		for (Text value : values) {
			context.write(new Text(cluster), value);
		}
	}
}