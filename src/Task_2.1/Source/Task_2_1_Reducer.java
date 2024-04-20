package Task_2_1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task_2_1_Reducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		ArrayList<Double> xList = new ArrayList<Double>();
		ArrayList<Double> yList = new ArrayList<Double>();
		
		for (Text value : values) {
			String[] tokens = value.toString().split(" ");
			xList.add(Double.valueOf(tokens[0]));
			yList.add(Double.valueOf(tokens[1]));
		}
		
		double sumX = 0;
		double sumY = 0;
		for (int i = 0; i<xList.size(); i++) {
			sumX += xList.get(i);
			sumY += yList.get(i);
		}
		
		double x_center = sumX / xList.size();
		double y_center = sumY / yList.size();
		
		Text value = new Text(String.valueOf(x_center) + " " + String.valueOf(y_center));
		context.write(key, value);
	}
}
