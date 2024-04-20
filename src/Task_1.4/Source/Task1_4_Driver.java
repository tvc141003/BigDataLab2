package Task1_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Task1_2.Task1_2_Driver;
import Task1_2.Task1_2_Mapper;
import Task1_2.Task1_2_Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

public class Task1_4_Driver {
	public enum ReducerCounters {
		    COUNTDISTINCT
	};
	public static void main(String[] args) throws Exception {
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "TF");
		
		
		
		// config job 1
		job1.setJarByClass(Task1_4_Driver.class);
		job1.setMapperClass(Task1_4_Mapper1.class);
		//job1.setCombinerClass(Task1_4_Reducer1.class);
		job1.setReducerClass(Task1_4_Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
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
		
		
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		if (job1.waitForCompletion(true) == true) {
			long reduce_input_groups = job1.getCounters()
                .findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_GROUPS")
                .getValue();
			
			Configuration conf2 = new Configuration();
            conf2.set("TotalDocuments", String.valueOf(reduce_input_groups));
            
         // config job2
    		Job job2 = Job.getInstance(conf2, "IDF");
    		job2.setJarByClass(Task1_4_Driver.class);
    		job2.setMapperClass(Task1_4_Mapper2.class);
//    		job2.setCombinerClass(Task_1_3_Reducer2.class);
    		job2.setReducerClass(Task1_4_Reducer2.class);
    		
    		job2.setMapOutputKeyClass(Text.class);
    		job2.setMapOutputValueClass(Text.class);
    		
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(Text.class);
    		
    		// config input job2
    		FileInputFormat.addInputPath(job2, new Path(args[args.length - 2] + "/part-r-00000"));
    		//config output job2
    		FileSystem fs2 = FileSystem.get(conf2); // delete file output when it exists
    		if (fs2.exists(new Path(args[args.length - 1]))) {
    			fs2.delete(new Path(args[args.length - 1]), true);
    		}
    		FileOutputFormat.setOutputPath(job2, new Path(args[args.length - 1]));
    		
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
//		Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "PlusMatrix");
//
//
//        job.setJarByClass(Task1_4_Driver.class);
//        job.setMapperClass(Task1_4_Mapper1.class);
//        job.setReducerClass(Task1_4_Reducer1.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//
//        for (int i = 0; i < args.length - 1; i++) {
//            FileInputFormat.addInputPath(job, new Path(args[i]));
//        }
//
//        FileSystem fs = FileSystem.get(conf); // delete file output when it exists
//        if (fs.exists(new Path(args[args.length - 1]))) {
//            fs.delete(new Path(args[args.length - 1]), true);
//        }
//
//        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
////        System.exit(job.waitForCompletion(true) ? 0 : 1);
//        if (job.waitForCompletion(true)) {
//            
//        }

        
	}
}

