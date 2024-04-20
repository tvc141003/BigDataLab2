package Task_1_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task_1_3_Reducer2 extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Float> maps = new HashMap<String, Float>();
		ArrayList<Float> list = new ArrayList<Float>();
		
		for (Text value : values) {
			StringTokenizer tokens = new StringTokenizer(value.toString());
			boolean isValue = false;
			String mapKey = "";
			float mapValue = 0;
			while (tokens.hasMoreTokens()) {
				String token = tokens.nextToken();
				// key
				if (isValue == false) {
					mapKey = token;
					isValue = true;
				} else {
					mapValue = Float.valueOf(token);
					isValue = false;
				}
			}
			list.add(mapValue);
			maps.put(mapKey, mapValue);
		}
		
		Collections.sort(list, Collections.reverseOrder());
		int count = 0;
		for (int i = 0; i<list.size(); i++) {
			if (count == 10) break;
			
			float target = list.get(i);
			for (Map.Entry<String, Float> entry : maps.entrySet()) {
				String pairKey = entry.getKey();
				float pairValue = entry.getValue();
				
				if (pairValue == target) {
					context.write(new Text(pairKey), new Text(String.valueOf(pairValue)));
					count++;
				}
				if (count == 10) break;
			}
		}
	}
}
