package pingcap.com;

public class MagicProtoBench {
	public native int sumInt(int x, int y);
	public native double sumDouble(double x, double y);
	public native byte[] alloc(int size);
}
