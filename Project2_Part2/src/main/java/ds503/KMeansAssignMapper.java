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
 * Map-only job:
 * Input: pointId,w,x,y,z
 * Output: pointId,w,x,y,z,clusterId,centerW,centerX,centerY,centerZ
 */
public class KMeansAssignMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final List<Center> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) {
        try {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new RuntimeException("No centers file in distributed cache.");
            }

            // Final centers file format: "c0\tw,x,y,z"  (from reducer output)
            Path centersPath = new Path(cacheFiles[0].toString());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    centersPath.getFileSystem(conf).open(centersPath)
            ))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\\t");
                    if (parts.length != 2) continue;
                    String cid = parts[0];
                    String[] nums = parts[1].split(",");
                    if (nums.length < 4) continue;
                    centers.add(new Center(cid,
                            Double.parseDouble(nums[0]),
                            Double.parseDouble(nums[1]),
                            Double.parseDouble(nums[2]),
                            Double.parseDouble(nums[3])));
                }
            }

            if (centers.isEmpty()) {
                throw new RuntimeException("Final centers loaded but had 0 centers.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load final centers", e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String[] p = value.toString().trim().split(",");
            if (p.length < 5) return;

            String pid = p[0];
            double w = Double.parseDouble(p[1]);
            double x = Double.parseDouble(p[2]);
            double y = Double.parseDouble(p[3]);
            double z = Double.parseDouble(p[4]);

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
                String out = pid + "," + w + "," + x + "," + y + "," + z + "," +
                        best.id + "," + best.w + "," + best.x + "," + best.y + "," + best.z;
                context.write(new Text(out), new Text(""));
            }
        } catch (Exception ignored) {}
    }

    static class Center {
        String id;
        double w, x, y, z;
        Center(String id, double w, double x, double y, double z) {
            this.id = id; this.w = w; this.x = x; this.y = y; this.z = z;
        }
    }
}