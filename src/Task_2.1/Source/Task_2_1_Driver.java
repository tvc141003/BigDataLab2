package Task_2_1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task_2_1_Driver {
	
	public static void updateCluster(Configuration conf, String pathString) throws IOException {
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
            	String xAxis = tokens[1];
            	String yAxis = tokens[2];
            	conf.set(Cluster, xAxis + " " + yAxis);
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
            	double new_xAxis = Double.valueOf(tokens[1]);
            	double new_yAxis = Double.valueOf(tokens[2]);
            	double old_xAxis = Double.valueOf(conf.get(Cluster).split(" ")[0]);
            	double old_yAxis = Double.valueOf(conf.get(Cluster).split(" ")[1]);
            	
            	if ((Math.abs(new_xAxis - old_xAxis) > threshold) || (Math.abs(new_yAxis - old_yAxis) > threshold)) return false;
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
		for (int i = 0; i<maxCluster; i++) {
			double xAxis = Math.random();
			double yAxis = Math.random();
			String cluster = "Cluster_" + String.valueOf(i);
			String value = String.valueOf(xAxis) + " " + String.valueOf(yAxis);
			conf.set(cluster, value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		final int MAX_EPOCHS = 20;
		final int MAX_CLUSTER = 3;
		final String INPUT = otherArgs[0];
		final String OUTPUT_TEMP = otherArgs[1] + "/part-r-00000";
		
		conf.setInt("MAX_CLUSTER", MAX_CLUSTER);
		initCluster(conf, MAX_CLUSTER);
		Job job = Job.getInstance(conf, "K_mean_itr_0");
		
		job.setJarByClass(Task_2_1_Driver.class);
		job.setMapperClass(Task_2_1_Mapper.class);
		job.setReducerClass(Task_2_1_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(INPUT));
		FileSystem fs = FileSystem.get(conf); // delete file output when it exists
		if (fs.exists(new Path(otherArgs[1]))) {
			fs.delete(new Path(otherArgs[1]), true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
		
		for (int i = 1; i<MAX_EPOCHS; i++) {
			if (isStopItr(conf, OUTPUT_TEMP) == true) break;
			
			System.out.println("Epochs: " + String.valueOf(i));
			
			updateCluster(conf, OUTPUT_TEMP);
			Job job_temp = Job.getInstance(conf, "K_mean_itr_" + String.valueOf(i));
			job_temp.setJarByClass(Task_2_1_Driver.class);
			job_temp.setMapperClass(Task_2_1_Mapper.class);
			job_temp.setReducerClass(Task_2_1_Reducer.class);
			
			job_temp.setMapOutputKeyClass(Text.class);
			job_temp.setMapOutputValueClass(Text.class);
			
			job_temp.setOutputKeyClass(Text.class);
			job_temp.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job_temp, new Path(INPUT));
			FileSystem fs_temp = FileSystem.get(conf); // delete file output when it exists
			if (fs_temp.exists(new Path(otherArgs[1]))) {
				fs_temp.delete(new Path(otherArgs[1]), true);
			}
			
			FileOutputFormat.setOutputPath(job_temp, new Path(otherArgs[1]));
			job_temp.waitForCompletion(true);
		}
		
		updateCluster(conf, OUTPUT_TEMP);
		Job job2 = Job.getInstance(conf, "WriteClassesFile");
		
		job2.setJarByClass(Task_2_1_Driver.class);
		job2.setMapperClass(Task_2_1_Mapper.class);
		job2.setReducerClass(Task_2_1_Reducer2.class);
		
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
		
		System.exit(job2.waitForCompletion(true) ? 0 :1);
	}
}
