package pingcap.com;

/**
 * The magic protocol.
 *
 * use to communicate with storage layer
 *
 * TODO: define Exceptions
 */
public class MagicProto {
	/**
	 * Get storage version.
	 *
	 * @return version string
	 */
	public native String version();

	/**
	 * Execute and open a query.
	 *
	 * @query the query string, eg: "SELECT * FROM test".
	 * @return token, must close later.
	 *
	 *  TODO: use AST query args
	 */
	public native long query(String query);

	/**
	 * Close an opened query, no matter it finished or not.
	 *
	 * @token the query token.
	 */
	public native void close(long token);

	/**
	 * Get schema from an opened query
	 *
	 * @token the query token.
	 * @return schema encoded by apache arrow
	 */
	public native byte[] schema(long token);

	/**
	 * Get data from an opened query
	 *
	 * @token create by `query`
	 * @return
	 *   block data (AKA: rows, or rowset, or record batch) encoded by apache arraw.
	 *   if nil means reached the end (query execution finished and all data are fetched)
	 */
	public native byte[] next(long token);
}
