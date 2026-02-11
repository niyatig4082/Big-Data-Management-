package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HobbyUsers {

    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        private static final String MY_HOBBY = "Swimming"; // change this
        private Text outKey = new Text();
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length > 4) {

                String nickName = fields[1].trim();
                String jobTitle = fields[2].trim();
                String hobby    = fields[4].trim();

                if (hobby.equalsIgnoreCase(MY_HOBBY)) {
                    outKey.set(nickName);
                    outVal.set(jobTitle);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hobby Users");

        job.setJarByClass(HobbyUsers.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}