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
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class TaskE {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final Text result = new Text();
        private final Text user = new Text();
        private final HashMap<String, Integer> counts = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            String userPagePair = values[1] + "," + values[2];
            counts.put(userPagePair, counts.getOrDefault(userPagePair, 0) + 1);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (HashMap.Entry<String, Integer> entry : counts.entrySet()) {
                String[] ids = entry.getKey().split(",");
                user.set(ids[0]);
                result.set(ids[1] + "," + entry.getValue());
                context.write(user, result);
            }
        }
    }

    public static class CounterReducer extends Reducer<Text,Text,Text,Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final Set<String> set = new HashSet<>();

            int sum = 0;
            for (Text val : values) {
                String[] fields = val.toString().split(",");
                sum += Integer.parseInt(fields[1]);
                set.add(fields[0]);
            }
            result.set(sum + "\t" + set.size());
            context.write(key, result);
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

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

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        System.out.println(duration);

        System.exit(0);
    }
}