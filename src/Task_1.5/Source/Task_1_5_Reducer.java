package Task_1_5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task_1_5_Reducer extends Reducer<Text, Text, Text, Text>{
	private Map<String, Float> termSum = new HashMap<String, Float>();
	private Map<String, Integer> termCount = new HashMap<String, Integer>();
	private Set<String> docSet = new HashSet<String>();
	
	private Text pairKey = new Text("");
	private Text pairValue = new Text("");
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text value : values) {
			String tokens[] = value.toString().split(" ");
			String term = tokens[0];
			String doc = tokens[1];
			float tf_idf = Float.valueOf(tokens[2]);
			
			docSet.add(doc);
			if (termSum.containsKey(term) == false) {
				termSum.put(term, tf_idf);
				termCount.put(term, 1);
			} else {
				float termValue = termSum.get(term);
				int counter = termCount.get(term);
				termSum.put(term, termValue + tf_idf);
				termCount.put(term, counter + 1);
			}
		}
		
		ArrayList<Float> lst = new ArrayList<Float>();
		for (Map.Entry<String, Float> entry : termSum.entrySet()) {
			String term = entry.getKey();
			float value = entry.getValue();
			int counter = termCount.get(term);
			
			termSum.put(term, value / counter);
			lst.add(value / counter);
		}
		
		Collections.sort(lst, Collections.reverseOrder());
		int count = 0;
		String writer = "";
		
		for (int i = 0; i<lst.size(); i++) {
			if (count == 5) break;
			
			float target = lst.get(i);
			for (Map.Entry<String, Float> entry : termSum.entrySet()) {
				String term = entry.getKey();
				float value = entry.getValue();
				
				if (value == target) {
					writer = writer + term + ": " + String.valueOf(value / docSet.size());
					count++;
					if (count < 5) writer = writer + ", ";
				}
				if (count == 5) break;
			}
		}
		pairKey.set(key.toString() + ": ");
		pairValue.set(writer);
		context.write(pairKey, pairValue);
	}
}
