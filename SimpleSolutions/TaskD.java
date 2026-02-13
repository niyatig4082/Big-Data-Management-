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


public class TaskD {

    public static class FollowMapper extends Mapper<Object, Text, Text, Text>{

        private final static Text f = new Text("F");
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            word.set(values[2]);
            context.write(word, f);
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {

        private final Text word = new Text();
        private final Text fields = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            word.set(values[0]);
            fields.set("P," + values[1]);
            context.write(word, fields);
        }
    }

    public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String nickname = "N/A";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("P,")) {
                    String[] fields = v.split(",");
                    nickname = fields[1];
                }
                else {
                    sum++;
                }
            }
            result.set(String.valueOf(sum));
            context.write(new Text(nickname), result);
        }
    }

    public static void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Activity Join");
        job.setJarByClass(TaskD.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FollowMapper.class);
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

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}