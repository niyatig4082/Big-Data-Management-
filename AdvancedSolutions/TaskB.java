import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;



public class TaskB {

    public static class ActivityMapper extends Mapper<Object, Text, Text, Text> {

        private final static Text a = new Text("A");
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            word.set(values[2]);
            context.write(word, a);
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {

        private final Text word = new Text();
        private final Text fields = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            word.set(values[0]);
            fields.set("P," + values[1] + "," + values[2]);
            context.write(word, fields);
        }
    }

    public static class AccessCounter extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            List<String> pages = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("A")) {
                    count++;
                } else if (v.startsWith("P,")) {
                    pages.add(v);
                }
            }

            // Emit summed activity if any
            if (count > 0) {
                result.set("A," + count);
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
            int sum = 0;
            String nickname = "N/A";
            String title = "N/A";

            for (Text val : values) {
                String v = val.toString();

                String[] fields = v.split(",");

                if (v.startsWith("P,")) {
                    nickname = fields[1];
                    title = fields [2];
                }
                else {
                    sum += Integer.parseInt(fields[1]);
                }
            }
            result.set(nickname + "\t" + title + "\t" + sum);
            context.write(key, result);
        }
    }

    public static class Top10Mapper extends Mapper<Object, Text, Text, Text> {
        private final Text id = new Text();
        private final Text result = new Text();
        private final PriorityQueue<String[]> top10 = new PriorityQueue<>(10, Comparator.comparingInt(a -> Integer.parseInt(a[3])));

        public void map(Object key, Text value, Context context) {
            String[] values = value.toString().split("\t");

            top10.add(values);

            if (top10.size() > 10) {
                top10.poll();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (String[] values : top10) {
                id.set(values[0]);
                result.set(String.join("\t", values));
                context.write(id, result);
            }
        }
    }

    public static class Top10Reducer extends Reducer<Text,Text,Text,Text> {

        private final PriorityQueue<String[]> top10 = new PriorityQueue<>(10, Comparator.comparingInt(a -> Integer.parseInt(a[3])));

        public void reduce(Text key, Iterable<Text> values, Context context) {

            for (Text val : values) {
                String[] attributes = val.toString().split("\t");

                top10.add(attributes);

                if (top10.size() > 10) {
                    top10.poll();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<String[]> result = new ArrayList<>(top10);
            result.sort((a,b) -> Integer.parseInt(b[3]) - Integer.parseInt(a[3]));

            for (String[] r : result) {
                context.write(new Text(r[0]), new Text(r[1] + "\t" + r[2]));
            }
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

        // Join/sum job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Activity Join");
        job.setJarByClass(TaskB.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ActivityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageMapper.class);
        job.setCombinerClass(AccessCounter.class);
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

        Path tmp = new Path(args[2] + "/tmp");
        FileOutputFormat.setOutputPath(job, tmp);

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        // Top 10 job
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Activity Join");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(Top10Mapper.class);
        job2.setReducerClass(Top10Reducer.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tmp);
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/output"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        System.out.println(duration);

        System.exit(0);
    }
}