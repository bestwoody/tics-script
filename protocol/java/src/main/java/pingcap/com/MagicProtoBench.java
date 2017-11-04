package pingcap.com;

public class MagicProtoBench {
	// Benchmark: JNI TPS
	public native int benchSumInt(int x, int y);
	public native double benchSumDouble(double x, double y);

	// Benchmark: JNI IO throughput
	public native byte[] benchAlloc(int size);

	// Benchmark: JNI arrow format IO throughput
	public native byte[] benchArrowArray(int size);

	// Benchmark: JNI arrow format IO throughput: TPCH lineite table
	public native byte[] benchLineitem(int size);
}
