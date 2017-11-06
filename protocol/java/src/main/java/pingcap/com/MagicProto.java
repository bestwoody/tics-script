package pingcap.com;

public class MagicProto {
	public native String version();

	// Mock interface: scan table, no push down
	public native byte[] scanAll(String table);

	// Scan table, push down: primary key range
	// TODO: key range type
	public native byte[] scan(String table, String lower, String upper);
}
