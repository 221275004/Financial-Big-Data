package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.util.HashSet;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

public class WordCount {

    public static class ContentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private HashSet<String> stopWords = new HashSet<>();
        private Pattern wordPattern = Pattern.compile("[a-zA-Z]+");

        @Override
        protected void setup(Context context) throws IOException {
            String stopWordsFilePath = context.getConfiguration().get("stopwords.file");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(stopWordsFilePath))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int firstCommaIndex = line.indexOf(',');
            int lastCommaIndex = line.lastIndexOf(',', line.length() - 3);

            if (firstCommaIndex != -1 && lastCommaIndex != -1 && firstCommaIndex < lastCommaIndex) {
                String extractedContent = line.substring(firstCommaIndex + 1, lastCommaIndex).trim();
                String[] words = extractedContent.toLowerCase().split("\\s+");

                for (String w : words) {
                    if (wordPattern.matcher(w).matches()) {
                        String cleanWord = w.toLowerCase();
                        if (!stopWords.contains(cleanWord)) {
                            word.set(cleanWord);
                            context.write(word, one);
                        }
                    }
                }
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
            while (!queue.isEmpty() && rank <= 100) {
                Map.Entry<String, Integer> entry = queue.poll();
                context.write(new Text(rank + ":" + entry.getKey() + "，" + entry.getValue()), null);
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WordCount <input path> <output path> <stopwords file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("stopwords.file", args[2]);

        Job job = Job.getInstance(conf, "extract and count words");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(ContentMapper.class);
        job.setReducerClass(ContentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

