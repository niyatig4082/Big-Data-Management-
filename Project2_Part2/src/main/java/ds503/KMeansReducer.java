package ds503;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Input values (from mapper OR combiner):
 * - If from mapper: w,x,y,z,1
 * - If from combiner: sumW,sumX,sumY,sumZ,count
 *
 * Reducer treats both the same (they are sums + count).
 *
 * Output: clusterId -> w,x,y,z
 */
public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sumW = 0, sumX = 0, sumY = 0, sumZ = 0;
        long count = 0;

        for (Text v : values) {
            String[] p = v.toString().split(",");
            if (p.length < 5) continue;

            sumW += Double.parseDouble(p[0]);
            sumX += Double.parseDouble(p[1]);
            sumY += Double.parseDouble(p[2]);
            sumZ += Double.parseDouble(p[3]);
            count += Long.parseLong(p[4]);
        }

        if (count == 0) return;

        double newW = sumW / count;
        double newX = sumX / count;
        double newY = sumY / count;
        double newZ = sumZ / count;

        context.write(key, new Text(newW + "," + newX + "," + newY + "," + newZ));
    }
}