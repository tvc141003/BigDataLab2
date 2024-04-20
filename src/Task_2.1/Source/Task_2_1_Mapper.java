package Task_2_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task_2_1_Mapper extends Mapper<Object, Text, Text, Text>{
	private static double getDistance(double x1, double y1, double x2, double y2) {
		double distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
		return distance;
	}
	
	private static String caculateCluster(Context context, double x, double y) {
		Configuration conf = context.getConfiguration();
		int max_cluster = conf.getInt("MAX_CLUSTER", 3);
		
		String returnValue = "";
		double distance = Double.MAX_VALUE;
		
		for (int i = 0; i<max_cluster; i++) {
			String name = "Cluster_" + String.valueOf(i);
			
			String cluster = conf.get(name);
			double cluster_x = Double.valueOf(cluster.split(" ")[0]);
			double cluster_y = Double.valueOf(cluster.split(" ")[1]);
			
			double distanceCluster = getDistance(x, y, cluster_x, cluster_y);
			if (distanceCluster < distance) {
				distance = distanceCluster;
				returnValue = name;
			}
		}
		return returnValue;
	}
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		double x = Double.valueOf(itr.nextToken());
		double y = Double.valueOf(itr.nextToken());
		String cluster = caculateCluster(context, x, y);
		
		Text pairKey = new Text(cluster);
		Text pairValue = new Text(String.valueOf(x) + " " + String.valueOf(y));
		context.write(pairKey, pairValue);
	}
}
