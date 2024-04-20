package Task2_3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.stream.Collectors;

public class Task2_3_Reducer extends Reducer<Text, Text, Text, Text> {

    private final Text newCenter = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	List<String> thislist = new ArrayList<String>();
    	int count = 0;
    	for (Text val : values) {
    		count += 1;
    		thislist.add(val.toString());
    	}
        // Compute new center for the cluster
        double [][] points = new double [count][10000];
        for (int i=0; i < thislist.size(); i++) {
            String[] parts = thislist.get(i).split("\\|");
            String[] tfidfValues = parts[1].split(",");
    	    double[] tfidfVector = new double[10000];
    	    for (int k = 0; k < tfidfVector.length; k++) {
    	    	tfidfVector[k] = 0.0;
            }
    	    for (int j=0; j<tfidfValues.length; j++) {
    	    	String[] tokens = tfidfValues[j].split(":");
    	    	tfidfVector[Integer.valueOf(tokens[0])] = Double.parseDouble(tokens[1]);
    		}
            points[i] = tfidfVector;
            
        }

        double[] newCenterVector = computeMean(points);

        // Emit new cluster center
        newCenter.set(Arrays.stream(newCenterVector)
            .mapToObj(String::valueOf)
            .collect(Collectors.joining(",")));
        context.write(key, newCenter);
    }

    private double[] computeMean(double[][] points) {
        // Compute mean of a list of vectors
        int dimensions = points[0].length;
        double[] mean = new double[dimensions];
        for (double[] point : points) {
            for (int i = 0; i < dimensions; i++) {
                mean[i] += point[i];
            }
        }
        for (int i = 0; i < dimensions; i++) {
            mean[i] /= points.length;
        }
        return mean;
    }
}
