import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskC {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private static final String MY_HOBBY = "Swimming";
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length > 4) {

                String nickName = fields[1].trim();
                String jobTitle = fields[2].trim();
                String hobby = fields[4].trim();

                if (hobby.equalsIgnoreCase(MY_HOBBY)) {
                    outKey.set(nickName);
                    outVal.set(jobTitle);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static void run(String[] args) throws Exception {
        Instant start = Instant.now();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hobby Users");

        job.setJarByClass(TaskC.class);
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

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        System.out.println(duration);

        System.exit(0);
    }
}