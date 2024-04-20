package Task2_3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Task2_3_Mapper extends Mapper<Object, Text, Text, Text>{
	private static Text closestCenter = new Text("");
	private static Text point = new Text("");
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int max_cluster = conf.getInt("MAX_CLUSTER", 5);
		
		// Parse input
	    String[] parts = value.toString().split("\\|");
	    // int docId = Integer.parseInt(parts[0]);
	    String[] tfidfValues = parts[1].split(",");
	    double[] tfidfVector = new double[10000];
	    for (int i = 0; i < tfidfVector.length; i++) {
	    	tfidfVector[i] = 0.0;
        }
	    for (int j=0; j<tfidfValues.length; j++) {
	    	String[] tokens = tfidfValues[j].split(":");
	    	tfidfVector[Integer.valueOf(tokens[0])] = Double.parseDouble(tokens[1]);
		}
	    
	    // Find closest cluster center
	    double minDistance = Double.MAX_VALUE;
	    int closestCluster = -1;
	
	    for (int i = 0; i < max_cluster; i++) {
	    	String name = "Cluster_" + String.valueOf(i);
			String cluster = conf.get(name);
			String[] values = cluster.split("\\s+");
			
			double[] center = new double[values.length];
			for (int j=0; j < values.length; j++) {
				center[j] = Double.parseDouble(values[j]);
			}
			
	        double distance = cosineSimilarity(tfidfVector, center);
	        if (distance < minDistance) {
	            minDistance = distance;
	            closestCluster = i;
	        }
	    }
	    // Emit (clusterId, point)
	    closestCenter.set("Cluster_" + String.valueOf(closestCluster));
	    point.set(value.toString());
	    context.write(closestCenter, point);
	}
	
	private double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    // Compute cosine similarity between two vectors
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
}
