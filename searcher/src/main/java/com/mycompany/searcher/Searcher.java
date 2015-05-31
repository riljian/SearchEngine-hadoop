/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.searcher;

/**
 *
 * @author ril
 */
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Searcher {
    
    private static final String KEYWORD = "search.keyword";
    
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{
        
        private IntWritable result = new IntWritable();
        private String keyword, filename;
        
        @Override
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            keyword = context.getConfiguration().get(KEYWORD);
            filename = ((FileSplit) context.getInputSplit()).getPath().toString();
        }
        
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            if (keyword == null) {
                return;
            }
            int index = value.find(keyword), counter = 0;
            while (index < value.getLength() && index != -1) {
                ++counter;
                index = value.find(keyword, index + keyword.length());
            }
            
            result.set(counter);
            context.write(new Text(filename), result);
        }
    }
    
    public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    private static void clearOutput(Configuration conf, Path path)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        System.out.print("Please input a keyword:\t");
        conf.set(KEYWORD, in.readLine());
        
        Job job = Job.getInstance(conf, "keyword search");
        
        job.setJarByClass(Searcher.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        clearOutput(conf, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        TimeUnit.SECONDS.sleep(1);
        
        in = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(args[1]+"/part-r-00000")), "UTF-8"));
        String line;
        HashMap<String, Integer> map = new HashMap();
        while ((line = in.readLine()) != null) {
            StringTokenizer tok = new StringTokenizer(line);
            map.put(tok.nextToken(), new Integer(tok.nextToken()));
        }
        
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
            public int compare(Map.Entry<String, Integer> entry1,
                    Map.Entry<String, Integer> entry2){
                return (entry2.getValue() - entry1.getValue());
            }
        });
        for (Map.Entry<String, Integer> entry:list) {
            in = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(entry.getKey())), "UTF-8"));
            System.out.println("\n" + in.readLine());
            System.out.println("\n" + in.readLine() + "\n");
        }
    }
}