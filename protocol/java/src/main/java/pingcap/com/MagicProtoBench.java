package pingcap.com;

public class MagicProtoBench {
	public native int benchSumInt(int x, int y);
	public native double benchSumDouble(double x, double y);
	public native byte[] benchAlloc(int size);
	public native byte[] benchArrowArray(int size);
}
