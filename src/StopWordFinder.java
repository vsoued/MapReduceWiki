import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class StopWordFinder {
    
    public static class StopMap extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        public void map(LongWritable whoknows, Text value, OutputCollector<Text, IntWritable> collector, 
                Reporter arg3) throws IOException {
         
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);                
           while(tokenizer.hasMoreTokens()){
               word.set(tokenizer.nextToken());
               collector.collect(word, one);
           }
        }
    }
    
    public static class StopReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> collector,
                Reporter reporter) throws IOException {
            
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
            }
            collector.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) {

        JobConf conf = new JobConf(StopWordFinder.class);
        conf.setJobName("CommonFriends");

        conf.setMapperClass(StopMap.class);
        conf.setReducerClass(StopReduce.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path("input"));
        FileOutputFormat.setOutputPath(conf, new Path("out"));

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
