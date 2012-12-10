



import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.commons.lang.StringUtils;

public class Query {
	
	//public static String query = "(camelcase and redirect)";
	public static HashMap<String,HashMap<Integer,String>> map = new HashMap<String,HashMap<Integer,String>>();
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word1 = new Text();
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//Query Processing
			List <String> or = new ArrayList<String>();
			List <String> and = new ArrayList<String>();
			List <String> not = new ArrayList<String>();
			
			Scanner reader = new Scanner(new FileInputStream("query.txt"));
			String query = reader.nextLine();
    		//System.out.println(query);
			
		
			String[] groups = StringUtils.substringsBetween(query,"(",")");
			
			for (int i=0; i<groups.length; i++) {
	
			    
				if (i>0) {
					String[] words = groups[i].split(" ");
					for (String word : words) {
						if (!word.equals("and") && !word.equals("or")) {
							not.add(word);
						}
					}
				} else {
//    				if (groups[i].contains(" and ")) {
//    					String[] words = groups[i].split(" ");
//    					for (String word : words) {
//    						if (!word.equals("and") && !word.equals("or")) {
//    							and.add(word);
//    						}
//    					}
//    				}
    				if (groups[i].contains(" or ")) {
    					String[] words = groups[i].split(" ");
    					for (String word : words) {
    						if (!word.equals("and") && !word.equals("or")) {
    							or.add(word);
    						}
    					}
    				} else {
    					String[] words = groups[i].split(" ");
    					for (String word : words) {
    						if (!word.equals("and") && !word.equals("or")) {
    							and.add(word);
    						}
    					}
    				}
				}
			}
			int orCount = 0;
			if (or.size()>0) {
				orCount = 1;
			}
			int andCount = and.size();
			String[] input = value.toString().split("\t");
			String word = input[0];
			String[] ids = (input[1].split(" "));
			for (int i=0;i<ids.length;i++) {
				String[] ids1 = ids[i].split("\\|");
				if (map.containsKey(ids1[0])) {
					map.get(ids1[0]).put(Integer.parseInt(ids1[1]),word);
				}
				else {
					HashMap<Integer,String> map1 = new HashMap<Integer,String>();
					map1.put(Integer.parseInt(ids1[1]),word);
					map.put(ids1[0],map1);
				}
			}
			
			if (and.contains(word)) {
				word1.set(word+"|and|"+andCount+"|"+orCount);
				for (int i=0;i<ids.length;i++) {
					String[] ids1 = ids[i].split("\\|");
					output.collect(new Text(ids1[0]),new Text(word1+"|"+ids1[1]));
				}
			}
			else if (or.contains(word)) {
				word1.set(word+"|or|"+andCount+"|"+orCount);
				for (int i=0;i<ids.length;i++) {
					String[] ids1 = ids[i].split("\\|");
					output.collect(new Text(ids1[0]),new Text(word1+"|"+ids1[1]));
				}
			}
			else if (not.contains(word)) {
				word1.set(word+"|not|"+andCount+"|"+orCount);
				for (int i=0;i<ids.length;i++) {
					String[] ids1 = ids[i].split("\\|");
					output.collect(new Text(ids1[0]),new Text(word1+"|"+ids1[1]));
				}
			}
		}
	}

public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		List <String> matches = new ArrayList<String>();
		List <String> existent = new ArrayList<String>();
		while (values.hasNext()) {
		    Text next = values.next();
		    if (!existent.contains(next.toString().split("\\|")[0])){
		        existent.add(next.toString().split("\\|")[0]);
		        matches.add(next.toString());
		    }
		}
		//System.out.println(map);
		//System.out.println("Matches: "+key+": "+matches);
		String[] attrs = matches.get(0).split("\\|");
		int andCount = Integer.parseInt(attrs[2]);
		int orCount = Integer.parseInt(attrs[3]);
		
		List <String> or = new ArrayList<String>();
		List <String> and = new ArrayList<String>();
		List <String> not = new ArrayList<String>();
		List <Integer> indices = new ArrayList<Integer>();
		
		for (String word : matches) {
			ArrayList<String> attrbts = new ArrayList<String>(Arrays.asList(word.split("\\|")));
			indices.add(Integer.parseInt(attrbts.get(4)));
			if (attrbts.contains("and")) {
				and.add(word);
			}
			else if (attrbts.contains("or")) {
				or.add(word);
			}
			else if (attrbts.contains("not")) {
				not.add(word);
			}
		}
		
        System.out.println(key+" "+ and +" "+andCount+" "+orCount);
		if (and.size()==andCount && or.size()>=orCount && not.size()==0) {
			//System.out.println(map.get(key.toString()));
			output.collect(key, new Text(getSnippet(indices,map.get(key.toString()))));
//            System.out.println(key+" "+ and +" "+andCount+" "+orCount);
		}
	}
	
	private static String getSnippet(List<Integer> indices,HashMap<Integer,String> map) {
	
		String snippet = "";
		for (Integer index : indices) {
			for (int i=index-4;i<=index+4;i++) {
				if (i>0 && i<map.size()+1) {
					snippet+=map.get(i)+" ";
				}
			}
			snippet+="\n";
		}
		return snippet;
	}
}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Query.class);
		conf.setJobName("Query");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);	
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("output2"));
		FileOutputFormat.setOutputPath(conf, new Path("output5"));
		JobClient.runJob(conf);
	}
}

