package Task2_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Task2_2_Mapper1 extends Mapper<Object, Text, Text, Text>{
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String[] parts = value.toString().split("\\s+");
		String clusterName = "Cluster_" + parts[0];
		String[] tokens = parts[1].split("\\|");
		
	    String[] tfidfValues = tokens[1].split(",");
	    double[] tfidfVector = new double[10000];
	    for (int i = 0; i < tfidfVector.length; i++) {
	    	tfidfVector[i] = 0.0;
        }
	    for (int j = 0; j < tfidfValues.length; j++) {
	    	String[] pairs = tfidfValues[j].split(":");
	    	tfidfVector[Integer.valueOf(pairs[0])] = Double.parseDouble(pairs[1]);
		}
	    
	    String cluster = conf.get(clusterName);
	    String[] centoid = cluster.split("\\s+");
	    double result = 0;
	    for (int i = 0; i < tfidfVector.length; i++) {
    		result += Math.pow(tfidfVector[i]- Double.parseDouble(centoid[i]), 2);
    	}
	    context.write(new Text(""), new Text(String.valueOf(result)));
	}
}
