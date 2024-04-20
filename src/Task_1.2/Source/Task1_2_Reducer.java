package Task1_2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1_2_Reducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	float sum = 0;
    	List <String> thislist = new ArrayList<String>();
    	for (Text val : values) {
    		StringTokenizer itr = new StringTokenizer(val.toString());
    		int index = 0;
    		while(itr.hasMoreTokens()) {
    			String token = itr.nextToken();
    			if (index == 2) {
    				sum += Float.parseFloat(token);
    			}
    			index += 1;
    		}
    		thislist.add(val.toString());
    	}
    	
    	if (sum >= (float) 3.0) {
    		for (String val : thislist) {
    			context.write(new Text(val), new Text(""));
    		}
    	}
    }
}
