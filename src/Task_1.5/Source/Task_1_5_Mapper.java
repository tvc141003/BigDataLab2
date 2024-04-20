package Task_1_5;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task_1_5_Mapper extends Mapper<Object, Text, Text, Text>{
	
	public static Text pairKey = new Text("");
	public static Text pairValue = new Text("");
	public Map<Integer, String> termsid = new HashMap<Integer, String>();
	public Map<Integer, String> docsid = new HashMap<Integer, String>();
	
	public static void readFiles(String filePath, Map<Integer, String> maps) {
        // Create a map
        try {
            File homedir = new File(System.getProperty("user.home"));
            File filename = new File(homedir, filePath);
            Scanner myReader = new Scanner(filename);
            int id = 1;
            while (myReader.hasNextLine()) {
              String data = myReader.nextLine().trim();
              maps.put(id, data);
              id += 1;
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
       
    }
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		readFiles("/Desktop/bbc/bbc.terms", termsid);
		readFiles("/Desktop/bbc/bbc.docs", docsid);
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			int termsIndex = Integer.valueOf(itr.nextToken());
			int docsIndex = Integer.valueOf(itr.nextToken());
			String tf_idf = itr.nextToken();
			
			String termsValue = termsid.get(termsIndex);
			String name = docsid.get(docsIndex);
			String docsValue = "";
			for (int i = 0; i<name.length(); i++) {
				if (name.charAt(i) == '.') break;
				docsValue += name.charAt(i);
			}
			pairKey.set(docsValue);
			pairValue.set(termsValue + " " + String.valueOf(docsIndex) + " " + tf_idf);
			context.write(pairKey, pairValue);
			break;
		}
	}
}
