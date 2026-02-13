import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class TaskE {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final Text page = new Text();
        private final Text user = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            user.set(values[1]);
            page.set(values[2]);
            context.write(user, page);
        }
    }

    public static class CounterReducer extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final Set<String> set = new HashSet<>();

            int count = 0;
            for (Text val : values) {
                count++;
                set.add(val.toString());
            }
            result.set(count + "\t" + set.size());
            context.write(key, result);
        }
    }

    public static void run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Accesses");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}