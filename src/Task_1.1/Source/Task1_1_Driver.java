package Task1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task1_1_Driver {
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
			
//			System.out.print(examplesDict.termid.get("service"));
			
			Job job = Job.getInstance(conf, "Examples");
			
			
			job.setJarByClass(Task1_1_Driver.class);
			job.setMapperClass(Task1_1_Mapper.class);
			job.setCombinerClass(Task1_1_Reducer.class);
			job.setReducerClass(Task1_1_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			for (int i = 0; i < args.length - 1; i++) {
				FileInputFormat.addInputPath(job, new Path(args[i]));
			}
			
			FileSystem fs = FileSystem.get(conf); // delete file output when it exists
			if (fs.exists(new Path(args[args.length - 1]))) {
				fs.delete(new Path(args[args.length - 1]), true);
			}
			
			FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
