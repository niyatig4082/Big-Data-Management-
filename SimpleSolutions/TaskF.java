import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class TaskF {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text user = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            user.set(values[2]);
            context.write(user, one);
        }
    }

    public static class CounterReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable ignored : values) {
                count++;
            }
            result.set(count);
            context.write(key, result);
            context.getCounter("Stats", "TotalFollowers").increment(count);
            context.getCounter("Stats", "TotalUsers").increment(1);
        }
    }

    public static class PopularMapper extends Mapper<Object, Text, Text, Text> {

        private final Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double avg = context.getConfiguration().getDouble("averageFollowers", 0);

            String[] values = value.toString().split("\t");

            if (Integer.parseInt(values[1]) > avg) {
                id.set(values[0]);
                context.write(id, new Text());
            }
        }
    }

    public static class PopularReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void run(String[] args) throws Exception {
        //Job 1: Get follower counts, and average
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Followers");
        job.setJarByClass(TaskF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        Path tmp = new Path(args[1] + "/tmp");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tmp);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        long totalFollowers = job.getCounters().findCounter("Stats", "TotalFollowers").getValue();
        long totalUsers = job.getCounters().findCounter("Stats", "TotalUsers").getValue();
        double average = (double) totalFollowers / totalUsers;

        //Job 2: Get popular user ids
        Configuration conf2 = new Configuration();
        conf2.setDouble("averageFollowers", average);

        Job job2 = Job.getInstance(conf2, "Get Popular");
        job2.setJarByClass(TaskF.class);
        job2.setMapperClass(PopularMapper.class);
        job2.setReducerClass(PopularReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tmp);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/output"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}