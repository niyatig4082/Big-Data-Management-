package ds503;

public class Point4D {
    public double w, x, y, z;

    public Point4D(double w, double x, double y, double z) {
        this.w = w; this.x = x; this.y = y; this.z = z;
    }

    public double dist(Point4D other) {
        return Math.sqrt(dist2(this.w, this.x, this.y, this.z, other.w, other.x, other.y, other.z));
    }

    public static double dist2(double w1, double x1, double y1, double z1,
                               double w2, double x2, double y2, double z2) {
        double dw = w1 - w2;
        double dx = x1 - x2;
        double dy = y1 - y2;
        double dz = z1 - z2;
        return dw*dw + dx*dx + dy*dy + dz*dz;
    }
}