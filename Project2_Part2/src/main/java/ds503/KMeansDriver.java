package ds503;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

/**
 * Args:
 * 0 pointsPath        (HDFS) e.g. /user/cs585/project2/input/points.csv
 * 1 centersInitPath   (HDFS) e.g. /user/cs585/project2/centers/centers_init.csv
 * 2 outBase           (HDFS) e.g. /user/cs585/project2/out
 * 3 R                 max iterations (int)
 * 4 threshold         convergence threshold (double), e.g. 0.001
 * 5 outputMode        "centers" or "assignments"
 *
 * Notes:
 * - Always writes centers each iteration under: outBase/centers/iter_i
 * - Writes convergence info under: outBase/meta/convergence.txt
 * - If outputMode=assignments, writes final assignments under: outBase/assignments/final
 */
public class KMeansDriver {

    private static final double EPS = 1e-9;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: KMeansDriver <pointsPath> <centersInitPath> <outBase> <R> <threshold> <outputMode>");
            System.exit(1);
        }

        for (int i = 0; i < args.length; i++) {
            System.out.println("args[" + i + "]=" + args[i]);
        }

        int off = 1;  // because args[0] is the main class name in this environment

        String pointsPath = args[off + 0];
        String centersInitPath = args[off + 1];
        String outBase = args[off + 2];
        int R = Integer.parseInt(args[off + 3]);
        double threshold = Double.parseDouble(args[off + 4]);
        String outputMode = args[off + 5].toLowerCase(java.util.Locale.ROOT);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path centersDir = new Path(outBase + "/centers");
        Path metaDir = new Path(outBase + "/meta");
        Path assignDir = new Path(outBase + "/assignments");

        // Clean output base if it already exists (safe for re-runs)
        deleteIfExists(fs, new Path(outBase));

        fs.mkdirs(centersDir);
        fs.mkdirs(metaDir);
        fs.mkdirs(assignDir);

        // iteration 0 centers = init centers file copied into centers/iter_0/part-r-00000 style
        Path iter0 = new Path(centersDir, "iter_0");
        fs.mkdirs(iter0);
        Path iter0Centers = new Path(iter0, "part-r-00000");
        copyFile(fs, new Path(centersInitPath), iter0Centers);

        boolean converged = false;
        int roundsRun = 0;
        int unchangedStreak = 0;
        double lastMaxShift = Double.POSITIVE_INFINITY;

        Path prevCentersFile = iter0Centers;

        for (int i = 1; i <= R; i++) {
            roundsRun = i;

            Path iterOut = new Path(centersDir, "iter_" + i);
            // Run one MapReduce iteration -> outputs K lines of new centers in part-r-00000
            runOneIteration(conf, pointsPath, prevCentersFile.toString(), iterOut.toString());

            Path newCentersFile = new Path(iterOut, "part-r-00000");

            // Convergence check: compare prev vs new
            CenterCompareResult cmp = compareCenters(fs, prevCentersFile, newCentersFile);
            lastMaxShift = cmp.maxShift;

            if (cmp.unchanged) {
                unchangedStreak++;
            } else {
                unchangedStreak = 0;
            }

            if (cmp.maxShift <= threshold) {
                converged = true;
                break;
            }
            if (unchangedStreak >= 2) {
                converged = true;
                break;
            }

            prevCentersFile = newCentersFile;
        }

        // Final centers file
        Path finalCentersFile = (roundsRun == 0) ? iter0Centers : new Path(centersDir + "/iter_" + roundsRun + "/part-r-00000");

        // Write convergence info
        writeConvergenceFile(fs, new Path(metaDir, "convergence.txt"),
                converged, roundsRun, R, threshold, lastMaxShift, unchangedStreak);

        // Output variation (e-ii): write final clustered points
        if ("assignments".equals(outputMode)) {
            Path finalAssignOut = new Path(assignDir, "final");
            runFinalAssignments(conf, pointsPath, finalCentersFile.toString(), finalAssignOut.toString());
        }

        System.out.println("DONE. converged=" + converged + " roundsRun=" + roundsRun + " maxShift=" + lastMaxShift);
    }

    private static void runOneIteration(Configuration conf, String pointsPath, String centersPath, String outPath) throws Exception {
        Job job = Job.getInstance(conf, "KMeans Iteration");
        job.setJarByClass(KMeansDriver.class);

        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class); // (d) optimization
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Distributed cache: centers file
        job.addCacheFile(new Path(centersPath).toUri());

        FileInputFormat.addInputPath(job, new Path(pointsPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Iteration job failed for output: " + outPath);
        }
    }

    private static void runFinalAssignments(Configuration conf, String pointsPath, String centersPath, String outPath) throws Exception {
        Job job = Job.getInstance(conf, "KMeans Final Assignments");
        job.setJarByClass(KMeansDriver.class);

        job.setMapperClass(KMeansAssignMapper.class);
        job.setNumReduceTasks(0); // map-only job

        job.setMapOutputKeyClass(LongWritable.class); // unused
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(centersPath).toUri());

        FileInputFormat.addInputPath(job, new Path(pointsPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Final assignment job failed for output: " + outPath);
        }
    }

    private static class CenterCompareResult {
        double maxShift;
        boolean unchanged;
        CenterCompareResult(double maxShift, boolean unchanged) {
            this.maxShift = maxShift;
            this.unchanged = unchanged;
        }
    }

    private static CenterCompareResult compareCenters(FileSystem fs, Path prevCenters, Path newCenters) throws IOException {
        Map<String, Point4D> prev = readCenters(fs, prevCenters);
        Map<String, Point4D> now = readCenters(fs, newCenters);

        double maxShift = 0.0;
        boolean unchanged = true;

        for (Map.Entry<String, Point4D> e : now.entrySet()) {
            String cid = e.getKey();
            Point4D cur = e.getValue();
            Point4D old = prev.get(cid);
            if (old == null) {
                unchanged = false;
                continue;
            }
            double shift = old.dist(cur);
            if (shift > maxShift) maxShift = shift;
            if (shift > EPS) unchanged = false;
        }

        return new CenterCompareResult(maxShift, unchanged);
    }

    private static Map<String, Point4D> readCenters(FileSystem fs, Path centersFile) throws IOException {
        Map<String, Point4D> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centersFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Output format from reducer: "c0\tw,x,y,z"
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\t");
                if (parts.length != 2) continue;
                String cid = parts[0];
                String[] nums = parts[1].split(",");
                if (nums.length < 4) continue;
                double w = Double.parseDouble(nums[0]);
                double x = Double.parseDouble(nums[1]);
                double y = Double.parseDouble(nums[2]);
                double z = Double.parseDouble(nums[3]);
                map.put(cid, new Point4D(w, x, y, z));
            }
        }
        return map;
    }

    private static void writeConvergenceFile(FileSystem fs, Path path,
                                             boolean converged, int roundsRun, int R, double threshold,
                                             double maxShift, int unchangedStreak) throws IOException {
        try (FSDataOutputStream out = fs.create(path, true)) {
            out.writeBytes("converged=" + converged + "\n");
            out.writeBytes("roundsRun=" + roundsRun + "\n");
            out.writeBytes("maxRoundsR=" + R + "\n");
            out.writeBytes("threshold=" + threshold + "\n");
            out.writeBytes("maxShiftLast=" + maxShift + "\n");
            out.writeBytes("unchangedStreak=" + unchangedStreak + "\n");
        }
    }

    private static void deleteIfExists(FileSystem fs, Path p) throws IOException {
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
    }

    private static void copyFile(FileSystem fs, Path src, Path dst) throws IOException {
        // src is an HDFS path; copy within HDFS
        FileUtil.copy(fs, src, fs, dst, false, fs.getConf());
    }
}