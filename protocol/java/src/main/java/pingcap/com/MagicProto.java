package pingcap.com;

public class MagicProto {
	public native String version();

	// TODO: AST form
	public native byte[] query(String query);
}
