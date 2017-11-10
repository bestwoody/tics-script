package pingcap.com;

public class MagicBlockStream {
	public MagicBlockStream(String handle) {
	}

	// TODO: use arrow schema class
	public String schema() {
		return "TODO: schema";
	}

	/*
	 * return: block data. if nil means this stream reached the end
	 */
	// TODO: use arrow RecordBatch
	public String next() {
		return "TODO: next";
	}
}
