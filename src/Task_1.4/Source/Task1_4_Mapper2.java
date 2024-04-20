package Task1_4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1_4_Mapper2 extends Mapper<Object, Text, Text, Text>{
	public static Text pairKey = new Text("");
	public static Text pairValue = new Text("");
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        
		StringTokenizer itr = new StringTokenizer(value.toString());
		int index = 0;
		String res = "";
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			if (index == 0) pairKey.set(token);
			else if (index == 1) res += token + " ";
			else res += token;
			index += 1;
		}
		pairValue.set(new Text(res));
		context.write(pairKey, pairValue);
	}
}