package Task1_4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1_4_Reducer1 extends Reducer<Text, Text, Text, Text>{
	private static Text result = new Text("");
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float count = 0;
		List<String> thislist = new ArrayList<String>();
		for (Text val: values) {
			String[] tokens = val.toString().split("@");
			String terms_id = tokens[0];
			Float frequentTerm = Float.valueOf(tokens[1]);
			count += frequentTerm;
			thislist.add(val.toString());
		}
		for (String element : thislist) {
			String[] tokens = element.split("@");
			String terms_id = tokens[0];
			Float frequentTerm = Float.valueOf(tokens[1]);
			Float termFrequency = (float) frequentTerm / count;
			result.set(terms_id + " " + key.toString() + " " + String.valueOf(termFrequency));
			context.write(result, new Text(""));
		}
	}
}
