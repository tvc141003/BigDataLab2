package Task1_2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1_2_Mapper extends Mapper<Object, Text, Text, Text>{
	public static Text word = new Text("");
    public static Text number = new Text("");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	while(itr.hasMoreTokens()) {
    		String token = itr.nextToken();
    		word.set(token);
            number.set(value.toString());
            break;
    	}
        context.write(word, number);   
    }
}