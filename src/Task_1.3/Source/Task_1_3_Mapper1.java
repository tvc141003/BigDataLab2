package Task_1_3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Task_1_3_Mapper1 extends Mapper<Object, Text, Text, FloatWritable>{
	public static Text pairKey = new Text("");
	public static FloatWritable pairValue = new FloatWritable(0);
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer tokens = new StringTokenizer(value.toString());
		while (tokens.hasMoreTokens()) {
			pairKey.set(tokens.nextToken());
			tokens.nextToken();
			String token = tokens.nextToken();
			pairValue.set(Float.valueOf(token));
			context.write(pairKey, pairValue);			
		}
		
	}
}
