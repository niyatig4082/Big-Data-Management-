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

import java.io.IOException;


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

    public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String nickname = "N/A";
            String title = "N/A";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("P,")) {
                    String[] fields = v.split(",");
                    nickname = fields[1];
                    title = fields [2];
                }
                else {
                    sum++;
                }
            }
            result.set(nickname + "\t" + title + "\t" + sum);
            context.write(key, result);
        }
    }

    public static class Top10Mapper extends Mapper<Object, Text, Text, Text> {
        private final Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            id.set(values[0]);
            context.write(id, value);
        }
    }

    public static class Top10Reducer extends Reducer<Text,Text,Text,Text> {

        private final PriorityQueue<String[]> topK = new PriorityQueue<>(10, Comparator.comparingInt(a -> Integer.parseInt(a[3])));

        public void reduce(Text key, Iterable<Text> values, Context context) {

            for (Text val : values) {
                String[] attributes =val.toString().split("\t");

                topK.add(attributes);

                if (topK.size() > 10) {
                    topK.poll();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<String[]> result = new ArrayList<>(topK);
            result.sort((a,b) -> Integer.parseInt(b[3]) - Integer.parseInt(a[3]));

            for (String[] r : result) {
                context.write(new Text(r[0]), new Text(r[1] + "\t" + r[2]));
            }
        }
    }

    public static void run(String[] args) throws Exception {
        // Join/sum job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Activity Join");
        job.setJarByClass(TaskB.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ActivityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageMapper.class);
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

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}