package Task2_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class Task2_3_Reducer2 extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Double> valueList = new ArrayList<>();

        // Iterate through all values for this key and add them to the list
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            for (String token : tokens) {
                valueList.add(Double.parseDouble(token));
            }
        }

        // Sort the values in descending order
        Collections.sort(valueList, Collections.reverseOrder());

        // Emit the top 2 values for this key
        String result = "";
        int n = Math.min(10, valueList.size());
        for (int i = 0; i < n; i++) {
        	if (i < n - 1)
        		result += valueList.get(i).toString() + ',';
        	else result += valueList.get(i).toString();
        }
        context.write(key, new Text(result));
	}
}