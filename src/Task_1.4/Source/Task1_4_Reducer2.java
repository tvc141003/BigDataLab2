package Task1_4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1_4_Reducer2 extends Reducer<Text, Text, Text, Text>{
//	private int totalDocuments = 0;
//	protected void setup(Context context) throws IOException, InterruptedException {
//        // Load total number of documents
//        totalDocuments = 0;
//        Configuration conf = context.getConfiguration();
//        totalDocuments = Integer.parseInt(conf.get("TotalDocuments"));
//    }
	private static Text result = new Text("");
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int totalDocuments = 0;
		
        Configuration conf = context.getConfiguration();
        String docs = conf.get("TotalDocuments");
        totalDocuments = Integer.valueOf(docs);
        int term_docs = 0;
        List<String> thislist = new ArrayList<String>();
		for (Text val : values) {
			term_docs += 1;
			thislist.add(val.toString());
		}
		for (String element : thislist) {
			String[] tokens = element.split(" ");
			String docs_id = tokens[0];
			Float tf = Float.valueOf(tokens[1]);
			Float idf = (float) Math.log(totalDocuments / term_docs); 
			Float tfidf = tf * idf;
			result.set(key.toString() + " " + docs_id + " " + String.valueOf(tfidf));
			context.write(result, new Text(""));
		}
	}
}
