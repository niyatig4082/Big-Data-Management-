import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
package org.example;

public class HobbyUsersMapOnly {

    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        private static final String MY_HOBBY = "Swimming";

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length > 4) {

                String nickName = fields[1].trim();
                String jobTitle = fields[2].trim();
                String hobby = fields[4].trim();

                if (hobby.equalsIgnoreCase(MY_HOBBY)) {
                    context.write(new Text(nickName), new Text(jobTitle));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hobby Users Map Only");

        job.setJarByClass(HobbyUsersMapOnly.class);
        job.setMapperClass(MyMapper.class);

        job.setNumReduceTasks(0);

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