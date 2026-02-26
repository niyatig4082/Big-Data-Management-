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

            // Centers file format: clusterId,w,x,y,z  (CSV)
            Path centersPath = new Path(cacheFiles[0].toString());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    centersPath.getFileSystem(conf).open(centersPath)
            ))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] p = line.trim().split(",");
                    if (p.length < 5) continue;
                    centers.add(new Center(
                            p[0],
                            Double.parseDouble(p[1]),
                            Double.parseDouble(p[2]),
                            Double.parseDouble(p[3]),
                            Double.parseDouble(p[4])
                    ));
                }
            }

            if (centers.isEmpty()) {
                throw new RuntimeException("Centers file loaded but had 0 centers.");
            }
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