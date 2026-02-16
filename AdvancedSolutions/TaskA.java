import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

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


public class TaskA {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable result = new IntWritable();
        private final Text word = new Text();
        private final HashMap<String, Integer> counts = new HashMap<>();

        public void map(Object key, Text value, Context context) {
            String[] values = value.toString().split(",");
            counts.put(values[4], counts.getOrDefault(values[4], 0) + 1);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (HashMap.Entry<String, Integer> entry : counts.entrySet()) {
                word.set(entry.getKey());
                result.set(entry.getValue());
                context.write(word, result);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorite hobby");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        System.out.println(duration);

        System.exit(0);
    }
}