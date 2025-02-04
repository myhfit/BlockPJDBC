package bp.jdbc;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import bp.data.BPXYDData;
import bp.data.BPXYData;
import bp.res.BPResourceJDBCLink;

public interface BPJDBCContext
{
	CompletionStage<Void> setJDBCLink(BPResourceJDBCLink link);

	CompletionStage<Boolean> connect();

	CompletionStage<Boolean> disconnect();

	CompletionStage<Boolean> commit();

	CompletionStage<Boolean> rollback();

	CompletionStage<BPXYDData> startQuery(String sql, Object[] params);

	CompletionStage<BPXYDData> startQuery(String sql, Object[] params, Consumer<BPXYDData> preparecallback);
	
	CompletionStage<BPXYData> query(String sql, Object[] params);

	CompletionStage<BPXYDData> resumeQuery(Consumer<BPXYDData> preparecallback);

	CompletionStage<BPXYDData> resumeQueryToEnd(Consumer<BPXYDData> preparecallback);

	CompletionStage<Integer> execute(String sql, Object[] params);

	CompletionStage<Boolean> interactiveBatchExecute(String sql, BiFunction<Integer, Exception, Object[]> callback);

	void stopRunSQL();

	void open();

	void close();
}
