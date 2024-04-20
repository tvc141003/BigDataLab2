package Task_1_3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task_1_3_Mapper2 extends Mapper<Object, Text, Text, Text>{
	public static Text pairKey = new Text("");
	public static Text pairValue = new Text("");
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer tokens = new StringTokenizer(value.toString());
		while (tokens.hasMoreTokens()) {
			String val = tokens.nextToken();
			val = val + " " + tokens.nextToken();
			pairValue.set(val);
			context.write(pairKey, pairValue);			
		}
		
	}
}