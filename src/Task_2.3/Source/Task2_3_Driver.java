package Task2_3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import Task2_2.Task2_2_Driver;
import Task2_2.Task2_2_Mapper;
import Task2_2.Task2_2_Mapper1;
import Task2_2.Task2_2_Mapper2;
import Task2_2.Task2_2_Reducer;
import Task2_2.Task2_2_Reducer1;
import Task2_2.Task2_2_Reducer2;
import Task2_2.Task2_2_Reducer3;
import Task_2_1.Task_2_1_Driver;
import Task_2_1.Task_2_1_Mapper;
import Task_2_1.Task_2_1_Reducer1;

public class Task2_3_Driver {
	
	public static void updateCluster(Configuration conf, String pathString) throws IOException {
		try {
			Path path = new Path(pathString);
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
			String line = br.readLine();
            while (line != null) {
            	if (line == "") continue;
//            	System.out.println(line);
            	String tokens[] = line.split("\\s+");
            	String Cluster = tokens[0];
            	String center = "";
            	String[] vector = tokens[1].split(",");
            	for (int i = 0; i < vector.length; i++)
            		center += vector[i] + " ";
            	conf.set(Cluster, center);
            	line = br.readLine();
            }
            
            br.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
	}
	
	public static boolean isStopItr(Configuration conf, String pathString) throws IOException {
		double threshold = 0.00000001f;
		try {
			Path path = new Path(pathString);
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
			String line = br.readLine();
            while (line != null) {
            	if (line == "") continue;
            	String tokens[] = line.split("\\s+");
            	String Cluster = tokens[0];
            	
            	double[] new_center = new double[10000];
            	String[] vector = tokens[1].split(",");
            	for (int i = 0; i < vector.length; i++)
            		new_center[i] = Double.parseDouble(vector[i]);
            	
            	String[] old_values = conf.get(Cluster).split(" ");

                double[] old_center = new double[old_values.length];

                for (int i = 0; i < old_values.length; i++)
                    old_center[i] = Double.parseDouble(old_values[i]);

                for (int i = 0; i < new_center.length; i++)
                    if (Math.abs(new_center[i] - old_center[i]) > threshold) return false;
            	line = br.readLine();
            }
            
            br.close();
            return true;
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
            return false;
          }
	}
	
	public static void initCluster(Configuration  conf, int maxCluster) {
		int vectorSize = 10000;
		for (int i = 0; i<maxCluster; i++) {
			String cluster = "Cluster_" + String.valueOf(i);
			String value = "";
			for (int j = 0;j < vectorSize; j++) {
				double num = Math.random();
				value += String.valueOf(num) + " ";
			}
			conf.set(cluster, value);
		}
	}
	
	public static void initializeCentroids(Configuration conf, String inputPath, int k) throws IOException {
        List<String> centroids = new ArrayList<>();
        
        // Read the input data to determine the initial set of centroids
        Path path = new Path(inputPath);
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
        String line;
        while ((line = br.readLine()) != null) {
        	int vectorSize = 10000;
        	String [] tokens = line.split("\\|");
        	String [] tfidfValues = tokens[1].split(",");
        	String result = "";
        	double[] tfidfVector = new double[vectorSize];
    	    for (int i = 0; i < tfidfVector.length; i++)
    	    	tfidfVector[i] = 0.0;
        
    	    for (int j=0; j<tfidfValues.length; j++) {
    	    	String[] pairs = tfidfValues[j].split(":");
    	    	tfidfVector[Integer.valueOf(pairs[0])] = Double.parseDouble(pairs[1]);
    		}
        	for (int i=0; i<tfidfVector.length; i++)
        		result += tfidfVector[i] + " ";
        	
            centroids.add(result); // Assuming each line represents a data point
        }
        br.close();
        
        // Select the first centroid randomly
        Random rand = new Random();
        String firstCentroid = centroids.remove(rand.nextInt(centroids.size()));
        // Add the first centroid to the configuration
        conf.set("Cluster_0", firstCentroid);
        
        // Select subsequent centroids using K-Means++ algorithm
        for (int i = 1; i < k; i++) {
            double[] distances = new double[centroids.size()];
            double totalDistance = 0.0;
            for (int j = 0; j < centroids.size(); j++) {
                double minDistance = Double.MAX_VALUE;
                for (int l = 0; l < i; l++) {
                    double distance = calculateDistance(centroids.get(j), conf.get("Cluster_" + l));
                    minDistance = Math.min(minDistance, distance);
                }
                distances[j] = minDistance;
                totalDistance += minDistance;
            }
            double threshold = rand.nextDouble() * totalDistance;
            double sum = 0.0;
            int nextCentroidIndex = 0;
            for (int j = 0; j < distances.length; j++) {
                sum += distances[j];
                if (sum >= threshold) {
                    nextCentroidIndex = j;
                    break;
                }
            }
            String nextCentroid = centroids.remove(nextCentroidIndex);
            conf.set("Cluster_" + i, nextCentroid);
        }
    }
	
	private static double calculateDistance(String point1, String point2) {
		String[] termsValues1 = point1.split(" ");
        String[] termsValues2 = point2.split(" ");

        // Create vectors to store term values
        double[] vector1 = new double[termsValues1.length];
        double[] vector2 = new double[termsValues2.length];
        
        for (int i=0; i<vector1.length; i++)
        	vector1[i] = Double.parseDouble(termsValues1[i]);
        for (int i=0; i<vector2.length; i++)
        	vector2[i] = Double.parseDouble(termsValues2[i]);
       
        // Calculate Euclidean distance
        double distance = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            distance += diff * diff;
        }
        return Math.sqrt(distance);
    }
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		final int MAX_EPOCHS = 10;
		final int MAX_CLUSTER = 5;
		final String INPUT = otherArgs[0];
		final String OUTPUT_TEMP = otherArgs[1] + "/part-r-00000";
		
		conf.setInt("MAX_CLUSTER", MAX_CLUSTER);
		initializeCentroids(conf, INPUT, MAX_CLUSTER);
		
		boolean flag = false;
		int i = 0;
		for (i = 0; i < MAX_EPOCHS; i++) {
			System.out.println(i);
			
			Job job1 = Job.getInstance(conf, "K_mean_itr_" + String.valueOf(i));
			job1.setJarByClass(Task2_3_Driver.class);
			job1.setMapperClass(Task2_3_Mapper.class);
			job1.setReducerClass(Task2_3_Reducer.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job1, new Path(INPUT));
			FileSystem fs1 = FileSystem.get(conf); // delete file output when it exists
			if (fs1.exists(new Path(otherArgs[1]))) {
				fs1.delete(new Path(otherArgs[1]), true);
			}
			
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
			job1.waitForCompletion(true);
			
			// Write classes file
			Job job2 = Job.getInstance(conf, "WriteClassesFile");
			
			job2.setJarByClass(Task2_3_Driver.class);
			job2.setMapperClass(Task2_3_Mapper.class);
			job2.setReducerClass(Task2_3_Reducer3.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(INPUT));
			FileSystem fs2 = FileSystem.get(conf); // delete file output when it exists
			if (fs2.exists(new Path(otherArgs[2]))) {
				fs2.delete(new Path(otherArgs[2]), true);
			}
			
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			
			job2.waitForCompletion(true);
			
			if (isStopItr(conf, OUTPUT_TEMP) == true) {
				flag = true;
				break;
			}
			
			System.out.println(flag);
			updateCluster(conf, OUTPUT_TEMP);
			// Write loss file
			Job job_loss = Job.getInstance(conf, "Loss Function" + String.valueOf(i));
			job_loss.setJarByClass(Task2_3_Driver.class);
			job_loss.setMapperClass(Task2_3_Mapper1.class);
			job_loss.setReducerClass(Task2_3_Reducer1.class);
			
			job_loss.setMapOutputKeyClass(Text.class);
			job_loss.setMapOutputValueClass(Text.class);
			
			job_loss.setOutputKeyClass(Text.class);
			job_loss.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job_loss, new Path(otherArgs[2] + "/part-r-00000"));
			FileSystem fs_loss = FileSystem.get(conf); // delete file output when it exists
			if (fs_loss.exists(new Path(otherArgs[3] + "_" +  String.valueOf(i)))) {
				fs_loss.delete(new Path(otherArgs[3] + "_" +  String.valueOf(i)), true);
			}
			
			FileOutputFormat.setOutputPath(job_loss, new Path(otherArgs[3] + "_" + String.valueOf(i)));
			job_loss.waitForCompletion(true);
			
			
			// Write sort file
			Job job_sort = Job.getInstance(conf, "Sort Function" + String.valueOf(i));
			job_sort.setJarByClass(Task2_3_Driver.class);
			job_sort.setMapperClass(Task2_3_Mapper2.class);
			job_sort.setReducerClass(Task2_3_Reducer2.class);
			
			job_sort.setMapOutputKeyClass(Text.class);
			job_sort.setMapOutputValueClass(Text.class);
			
			job_sort.setOutputKeyClass(Text.class);
			job_sort.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job_sort, new Path(otherArgs[1] + "/part-r-00000"));
			FileSystem fs_sort = FileSystem.get(conf); // delete file output when it exists
			if (fs_sort.exists(new Path(otherArgs[4] + "_" + String.valueOf(i)))) {
				fs_sort.delete(new Path(otherArgs[4] + "_" +String.valueOf(i)), true);
			}
			
			FileOutputFormat.setOutputPath(job_sort, new Path(otherArgs[4] + "_" + String.valueOf(i)));
			job_sort.waitForCompletion(true);
		}
		
		if (flag == true) {
			// Write loss file
			updateCluster(conf, OUTPUT_TEMP);
			
			Job job_loss = Job.getInstance(conf, "Loss Function" + String.valueOf(i));
			job_loss.setJarByClass(Task2_3_Driver.class);
			job_loss.setMapperClass(Task2_3_Mapper1.class);
			job_loss.setReducerClass(Task2_3_Reducer1.class);
			
			job_loss.setMapOutputKeyClass(Text.class);
			job_loss.setMapOutputValueClass(Text.class);
			
			job_loss.setOutputKeyClass(Text.class);
			job_loss.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job_loss, new Path(otherArgs[2] + "/part-r-00000"));
			FileSystem fs_loss = FileSystem.get(conf); // delete file output when it exists
			if (fs_loss.exists(new Path(otherArgs[3] + "_" +  String.valueOf(i)))) {
				fs_loss.delete(new Path(otherArgs[3] + "_" +  String.valueOf(i)), true);
			}
			
			FileOutputFormat.setOutputPath(job_loss, new Path(otherArgs[3] + "_" + String.valueOf(i)));
			job_loss.waitForCompletion(true);
			
			
			// Write sort file
			Job job_sort = Job.getInstance(conf, "Sort Function" + String.valueOf(i));
			job_sort.setJarByClass(Task2_3_Driver.class);
			job_sort.setMapperClass(Task2_3_Mapper2.class);
			job_sort.setReducerClass(Task2_3_Reducer2.class);
			
			job_sort.setMapOutputKeyClass(Text.class);
			job_sort.setMapOutputValueClass(Text.class);
			
			job_sort.setOutputKeyClass(Text.class);
			job_sort.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job_sort, new Path(otherArgs[1] + "/part-r-00000"));
			FileSystem fs_sort = FileSystem.get(conf); // delete file output when it exists
			if (fs_sort.exists(new Path(otherArgs[4] + "_" + String.valueOf(i)))) {
				fs_sort.delete(new Path(otherArgs[4] + "_" +String.valueOf(i)), true);
			}
			
			FileOutputFormat.setOutputPath(job_sort, new Path(otherArgs[4] + "_" + String.valueOf(i)));
			job_sort.waitForCompletion(true);
		}
		
		System.exit(1);
	}
}
