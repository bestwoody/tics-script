package pingcap.com;

public class MagicProto {
	public native String version();

	/*
	 * return: handle for createBlockStream
	 */
	// TODO: use AST query
	public native String query(String query);
}
