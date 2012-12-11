

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.lang.StringUtils;

public class InvertedIndex {
	
	public static boolean newtitle = false;
	public static String id_number = "";
	public static int position = 0;

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text id = new Text();
		private Text word = new Text();
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			//Scanner reader = new Scanner(new FileInputStream("stopwords.txt"));
    		//List <String> stopwords = new ArrayList<String>();
    		//while (reader.hasNextLine()) {      // while there is another token to read
    		//    stopwords.add(reader.nextLine().toLowerCase());   // reads in the String tokens "Hello" "CSstudents" 
    		//}
            //stopwords.set(0, "a");
		    List<String> stopwords = new ArrayList<String>(Arrays.asList(Stopwords.stopwords));
		    
			String line = value.toString();
			if (line.contains("</title>")) {
				newtitle = true;
				
			}
			//String id_number = "";
			if (newtitle && line.contains("<id>")) {
				newtitle =false;
				id_number = StringUtils.substringBetween(line, "<id>","</id>");
				id.set(id_number);
				position = 0;
				
			}
			line = line.replaceAll("<[^>]*>", " ").replaceAll("\\[[^\\[]*\\]", " ").replaceAll("\\{[^\\{]*\\}", " ").replaceAll("[^a-zA-Z]+", " ");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word1 = tokenizer.nextToken().toLowerCase();
				if (!stopwords.contains(word1) && !id_number.equals("")) {
					position+=1;
					id.set("");
    				word.set(word1);
    				//System.out.print(word1);
    				//System.out.print("::");
    				//System.out.println(id_number+"|"+position+" ");
    				id.set(id_number+"|"+position);
    				
    				output.collect(word, id);
				}
			}
		}
	}

public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String ids = "";
		while (values.hasNext()) {
			ids += values.next() + " ";
		}
		
		//if (!ids.split("|")[0].equals(" ")){
		Text article_ids = new Text();
		article_ids.set(ids.substring(0,ids.length()-1));
		output.collect(key, article_ids);
		
	}
}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("InvertedIndex");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);	
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		JobClient.runJob(conf);
	}
}