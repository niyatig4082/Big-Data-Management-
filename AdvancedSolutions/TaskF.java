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
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;


public class TaskF {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text user = new Text();
        private final IntWritable result = new IntWritable();
        private final HashMap<String, Integer> counts = new HashMap<>();

        public void map(Object key, Text value, Context context) {
            String[] values = value.toString().split(",");
            counts.put(values[2], counts.getOrDefault(values[2], 0) + 1);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (HashMap.Entry<String, Integer> entry : counts.entrySet()) {
                user.set(entry.getKey());
                result.set(entry.getValue());
                context.write(user, result);
            }
        }
    }

    public static class CounterReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            context.getCounter("Stats", "TotalFollowers").increment(sum);
            context.getCounter("Stats", "TotalUsers").increment(1);
        }
    }

    public static class PopularMapper extends Mapper<Object, Text, Text, Text> {

        private final Text id = new Text();

        private final Text empty = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double avg = context.getConfiguration().getDouble("averageFollowers", 0);

            String[] values = value.toString().split("\t");

            if (Integer.parseInt(values[1]) > avg) {
                id.set(values[0]);
                context.write(id, empty);
            }
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

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
        job2.setNumReduceTasks(0);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tmp);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/output"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        System.out.println(duration);

        System.exit(0);
    }
}