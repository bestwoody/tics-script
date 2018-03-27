package pingcap.com;

/**
 * TheFlash protocol.
 *
 * use to communicate with storage layer
 * thread safe, we should only create one instance on one config (file).
 * operations on one query are not thread safe
 */
public class TheFlashProto {
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
	 * @return error string, is null if all good.
	 */
	public static class InitResult {
		String error;
	}
	public native InitResult init(String config);

	/**
	 * Destory this class instance.
	 * must not call any method after this call..
	 *
	 * @return error string, is null if all good.
	 */
	public static class FinishResult {
		String error;
	}
	public native FinishResult finish();

	/**
	 * List all unclosed querys (just the querys start with TheFlashProtocol)
	 *
	 * @return array of query tokens
	 */
	public static class QuerysResult {
		String error;
		long[] tokens;
	}
	public native QuerysResult querys();

	/**
	 * Execute and open a query.
	 *
	 * @query the query string, eg: "SELECT * FROM test".
	 * @return
	 *   QueryResult instance, if return null means some wrong with the code.
	 *   QueryResult.error, is null if all good.
	 *   QueryResult.token, must close later.
	 *
	 * TODO: use AST query args
	 */
	public static class QueryResult {
		public String error;
		public long token;
	};
	public native QueryResult query(String query);

	/**
	 * Check and get executing error.
	 *
	 * @token the query token.
	 * @return
	 *   error detail string.
	 *   return null if no error ocurred.
	 */
	public native String error(long token);

	/**
	 * Close an opened query, no matter it finished or not.
	 * if query result data are not all fetched, close means cancal the query.
	 *
	 * @token the query token.
	 * @return
	 *   return nothing
	 *   call "error" to check and get detail error info.
	 */
	public native void close(long token);

	/**
	 * Get schema from an opened query
	 *
	 * @token the query token.
	 * @return
	 *   schema encoded by apache arrow
	 *   if return null means an error ocurred, call "error" to fetch detail error info.
	 */
	public native byte[] schema(long token);

	/**
	 * Get data from an opened query
	 *
	 * @token create by `query`
	 * @return
	 *   block data (AKA: rows, or rowset, or record batch) encoded by apache arraw.
	 *   if return nil means reached the end (query execution finished and all data are fetched)
	 *   call "error" to check and get detail error info.
	 */
	public native byte[] next(long token);
}
