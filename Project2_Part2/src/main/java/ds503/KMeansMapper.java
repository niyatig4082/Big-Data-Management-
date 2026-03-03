package ds503;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Input line: pointId,w,x,y,z
 * Output: clusterId -> w,x,y,z,1
 */
public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final List<Center> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) {
        try {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new RuntimeException("No centers file in distributed cache.");
            }

            // In local mode, Hadoop creates a local symlink named like the original file (often part-r-00000)
            // Safest: read the first cache file as a *local file path*.
            Path localPath = new Path(cacheFiles[0].getPath()); // <-- key change

            List<Center> loaded = new ArrayList<>();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    localPath.getFileSystem(conf).open(localPath)
            ))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String cid;
                    String[] nums;

                    if (line.contains("\t")) {
                        // reducer output format: c0\tw,x,y,z
                        String[] parts = line.split("\\t");
                        if (parts.length != 2) continue;
                        cid = parts[0].trim();
                        nums = parts[1].split(",");
                    } else {
                        // seed/init file format: c0,w,x,y,z
                        String[] parts = line.split(",");
                        if (parts.length < 5) continue;
                        cid = parts[0].trim();
                        nums = new String[]{parts[1], parts[2], parts[3], parts[4]};
                    }

                    if (nums.length < 4) continue;

                    double w = Double.parseDouble(nums[0]);
                    double x = Double.parseDouble(nums[1]);
                    double y = Double.parseDouble(nums[2]);
                    double z = Double.parseDouble(nums[3]);

                    loaded.add(new Center(cid, w, x, y, z));
                }
            }

            if (loaded.isEmpty()) {
                throw new RuntimeException("Centers file loaded but had 0 centers. cache=" + cacheFiles[0]);
            }

            centers.clear();
            centers.addAll(loaded);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load centers", e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String[] p = value.toString().trim().split(",");
            if (p.length < 4) return;

            double w = Double.parseDouble(p[0]);
            double x = Double.parseDouble(p[1]);
            double y = Double.parseDouble(p[2]);
            double z = Double.parseDouble(p[3]);

            Center best = null;
            double bestD = Double.POSITIVE_INFINITY;

            for (Center c : centers) {
                double d = Point4D.dist2(w, x, y, z, c.w, c.x, c.y, c.z);
                if (d < bestD) {
                    bestD = d;
                    best = c;
                }
            }

            if (best != null) {
                // w,x,y,z,count
                context.write(new Text(best.id), new Text(w + "," + x + "," + y + "," + z + ",1"));
            }
        } catch (Exception ignored) {
            // skip malformed line
        }
    }

    static class Center {
        String id;
        double w, x, y, z;
        Center(String id, double w, double x, double y, double z) {
            this.id = id;
            this.w = w; this.x = x; this.y = y; this.z = z;
        }
    }
}