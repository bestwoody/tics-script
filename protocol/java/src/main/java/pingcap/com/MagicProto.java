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
	 * can call before `init`
	 *
	 * @return version string
	 */
	public native String version();

	/**
	 * Init this class instance.
	 * must call before any other calls.
	 *
	 * @config the config file, or the root config file if there are more then one config file
	 */
	public native void init(String config);

	/**
	 * Destory this class instance.
	 * must not call any method after this call..
	 */
	public native void finish();

	/**
	 * Execute and open a query.
	 *
	 * @query the query string, eg: "SELECT * FROM test".
	 * @return
	 *   token, must close later.
	 *   if token < 0, means an error ocurred.
	 *
	 * TODO: use AST query args
	 * TODO: define error codes, and a method to get error detail
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
