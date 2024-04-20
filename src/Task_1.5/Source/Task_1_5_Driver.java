package Task_1_5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task_1_5_Driver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SumMatrix");
		
		
		job.setJarByClass(Task_1_5_Driver.class);
		job.setMapperClass(Task_1_5_Mapper.class);
		job.setReducerClass(Task_1_5_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
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
