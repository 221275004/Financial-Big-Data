package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class StockCount {

    public static class ContentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean isFirstLine = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Ignore the first line
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            String line = value.toString();
            int lastCommaIndex = line.lastIndexOf(',');

            if (lastCommaIndex != -1 && lastCommaIndex < line.length() - 1) {
                String extractedContent = line.substring(lastCommaIndex + 1).trim();
                word.set(extractedContent);
                context.write(word, one);
            }
        }
    }

    public static class ContentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<String, Integer>> queue = new PriorityQueue<>(
                (a, b) -> b.getValue().compareTo(a.getValue())
            );

            queue.addAll(countMap.entrySet());

            int rank = 1;
            while (!queue.isEmpty()) {
                Map.Entry<String, Integer> entry = queue.poll();
                context.write(new Text(rank + ":" + entry.getKey() + "，" + entry.getValue()), null);
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StockCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "extract and count content");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(ContentMapper.class);
        job.setReducerClass(ContentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
