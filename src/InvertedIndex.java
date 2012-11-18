import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.w3c.dom.Text;


public class InvertedIndex {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    	private Text Key = new Text();
    	private Text article = new Text();
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		
    		Scanner reader = new Scanner(new FileInputStream("/home/g/grad/mmercald/hadoop-1.0.4/stopwords.txt"));
    		List <String> stopwords = new ArrayList<String>();
    		while (reader.hasNext()) {      // while there is another token to read
    		    stopwords.add(reader.next().toLowerCase());   // reads in the String tokens "Hello" "CSstudents" 
    		}
    		
    		//Assuming the input(value) is a line, if input is a file then we'll add another loop to go over each line
    		String line = value.toString(); //Needs to be splitted by " ","<" and ">"
    		
   			StringTokenizer tokenizer = new StringTokenizer(line);
   			article.set(value);
   			//Loop though each word, return it as value only if it not a tag or stop word
   			while (tokenizer.hasMoreTokens()) {
   				String word = tokenizer.nextToken().toLowerCase();
   				char firstChar = word.charAt(0);
    			if (!stopwords.contains(word) && firstChar != '/' && firstChar != '<') {
    				Key.set(word);
    			}
    			context.write(Key,article);
    		}
    	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		
    		List <Text> articles = new ArrayList<Text>();
    		for (Text article : values) {
    			articles.add(article);
    			    		context.write(key,article);
    		}

    	}
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "InvertedIndex");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        job.waitForCompletion(true);
     }
            
    }