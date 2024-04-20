package Task_1_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task_1_3_Driver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "SumTerms");
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "SortValues");
		
		// config job 1
		job1.setJarByClass(Task_1_3_Driver.class);
		job1.setMapperClass(Task_1_3_Mapper1.class);
		job1.setCombinerClass(Task_1_3_Reducer.class);
		job1.setReducerClass(Task_1_3_Reducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		
		// config job2
		job2.setJarByClass(Task_1_3_Driver.class);
		job2.setMapperClass(Task_1_3_Mapper2.class);
//		job2.setCombinerClass(Task_1_3_Reducer2.class);
		job2.setReducerClass(Task_1_3_Reducer2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		// config input job1
		for (int i = 0; i < args.length - 2; i++) {
			FileInputFormat.addInputPath(job1, new Path(args[i]));
		}
		
		FileSystem fs = FileSystem.get(conf1); // delete file output when it exists
		if (fs.exists(new Path(args[args.length - 2]))) {
			fs.delete(new Path(args[args.length - 2]), true);
		}
		// config output job1
		FileOutputFormat.setOutputPath(job1, new Path(args[args.length - 2]));
		
		// config input job2
		FileInputFormat.addInputPath(job2, new Path(args[args.length - 2] + "/part-r-00000"));
		//config output job2
		FileSystem fs2 = FileSystem.get(conf2); // delete file output when it exists
		if (fs2.exists(new Path(args[args.length - 1]))) {
			fs2.delete(new Path(args[args.length - 1]), true);
		}
		FileOutputFormat.setOutputPath(job2, new Path(args[args.length - 1]));
		
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		if (job1.waitForCompletion(true) == true) {
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
