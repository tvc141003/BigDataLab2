package Task1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Task1_1_Mapper extends Mapper<Object, Text, Text, FloatWritable>{
	private Text word = new Text("");
	private FloatWritable one = new FloatWritable((float) 1.0);
	private Map<String, String> terms_id = new HashMap<String, String>();
	private Map<String, String> docs_id = new HashMap<String, String>();
	private Map<String, String> stop_words = new HashMap<String, String>();
	
	public static void readFiles(String filePath, Map<String, String> maps) {
        // Create a map
        try {
            File homedir = new File(System.getProperty("user.home"));
            File filename = new File(homedir, filePath);
            Scanner myReader = new Scanner(filename);
            int id = 0;
            while (myReader.hasNextLine()) {
              String data = myReader.nextLine().trim();
              id += 1;
              maps.put(data, String.valueOf(id));
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
       
    }
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		readFiles("/Desktop/bbc/bbc.terms", terms_id);
		readFiles("/Desktop/bbc/bbc.docs", docs_id);
		readFiles("/Desktop/stopwords.txt", stop_words);
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		
		String parent = fileSplit.getPath().getParent().getName();
		String fileName = fileSplit.getPath().getName();
	
		String path = parent + ".";
		for (int i = 0; i<fileName.length() - 4; i++)
			path += fileName.charAt(i);
		
		String docs = docs_id.get(path);
		
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken().toLowerCase();
//			token = token.replaceAll("[^a-zA-Z]", "").trim();
			if (stop_words.get(token) != null) continue;
			String term = terms_id.get(token);
			if (term == null) continue;
			
			String keyMap = term + " " + docs;
			word.set(keyMap);
//			word.set(token + " " + path);
			
			context.write(word, one);
		}
		
	}
}
