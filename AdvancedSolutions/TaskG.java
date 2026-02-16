import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


public class TaskG {

    public static class ActivityMapper extends Mapper<Object, Text, Text, Text> {

        private final Text time = new Text();
        private final Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            id.set(values[1]);
            time.set("A," + values[4]);
            context.write(id, time);
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {

        private final Text id = new Text();
        private final Text name = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            id.set(values[0]);
            name.set("P," + values[1]);
            context.write(id, name);
        }
    }

    public static class MaxCombiner extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int max = 0;
            List<String> pages = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("A,")) {
                    max = Math.max(max, Integer.parseInt(v.split(",")[1]));
                } else {
                    pages.add(v);
                }
            }

            // Emit summed activity if any
            if (max > 0) {
                result.set("A," + max);
                context.write(key, result);
            }

            // Emit page records unchanged
            for (String p : pages) {
                result.set(p);
                context.write(key, result);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            String nickname = "N/A";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("P,")) {
                    String[] fields = v.split(",");
                    nickname = fields[1];
                }
                else {
                    max = Math.max(max, Integer.parseInt(v.split(",")[1]));
                }
            }
            if (max < (1_000_000 - 90 * 24)) {
                result.set(nickname);
                context.write(key, result);
            }
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Outdated Profiles");
        job.setJarByClass(TaskG.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ActivityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageMapper.class);
        job.setCombinerClass(MaxCombiner.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

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