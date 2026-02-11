import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TaskB_Top10PopularPages extends Configured implements Tool {

       //JOB 1: COUNT PAGE ACCESSES

    public static class AccessCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pageId = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");

            // ActivityLog format:
            // ActionId, ByWho, WhatPage, ActionType, ActionTime
            if (fields.length == 5 && !fields[2].equals("WhatPage")) {
                pageId.set(fields[2].trim());
                context.write(pageId, one);
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

       //JOB 2: GLOBAL TOP 10 + DISTRIBUTED CACHE JOIN

    public static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private Map<String, String[]> pageInfo = new HashMap<>();

        private PriorityQueue<Map.Entry<String, Integer>> topK =
                new PriorityQueue<>(10, Comparator.comparingInt(Map.Entry::getValue));

        @Override
        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    BufferedReader br = new BufferedReader(
                            new FileReader(new File("./CircleNetPage.csv")));

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] fields = line.split(",");
                        if (fields.length >= 3 && !fields[0].equals("ID")) {
                            pageInfo.put(fields[0].trim(),
                                    new String[]{fields[1].trim(), fields[2].trim()});
                        }
                    }
                    br.close();
                }
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            Map.Entry<String, Integer> entry =
                    new AbstractMap.SimpleEntry<>(key.toString(), count);

            topK.add(entry);
            if (topK.size() > 10) {
                topK.poll(); // remove smallest
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            List<Map.Entry<String, Integer>> result =
                    new ArrayList<>(topK);

            result.sort((a, b) -> b.getValue() - a.getValue());

            for (Map.Entry<String, Integer> entry : result) {
                String id = entry.getKey();
                int count = entry.getValue();

                String nickname = "N/A";
                String jobTitle = "N/A";

                if (pageInfo.containsKey(id)) {
                    nickname = pageInfo.get(id)[0];
                    jobTitle = pageInfo.get(id)[1];
                }

                context.write(new Text(id),
                        new Text(nickname + "\t" + jobTitle + "\t" + count));
            }
        }
    }

       //DRIVER

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: TaskB <ActivityLog> <CircleNetPage> <tmpOut> <finalOut>");
            return -1;
        }

        Configuration conf = getConf();

           //JOB 1

        Job job1 = Job.getInstance(conf, "Page Access Count");
        job1.setJarByClass(TaskB_Top10PopularPages.class);

        job1.setMapperClass(AccessCountMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

           //JOB 2
        Job job2 = Job.getInstance(conf, "Top 10 Pages");
        job2.setJarByClass(TaskB_Top10PopularPages.class);

        job2.setReducerClass(Top10Reducer.class);
        job2.setNumReduceTasks(1); // global top-10

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.addCacheFile(new URI(args[1] + "#CircleNetPage.csv"));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TaskB_Top10PopularPages(), args);
        System.exit(res);
    }
}
