package Task2_3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Task2_3_Mapper2 extends Mapper<Object, Text, Text, Text>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String [] tokens = value.toString().split("\\s+");
		context.write(new Text(tokens[0]), new Text(tokens[1]));
	}

}
