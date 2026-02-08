package bp.jdbc;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import bp.data.BPXYDData;
import bp.data.BPXYData;
import bp.env.BPEnv;
import bp.res.BPResourceJDBCLink;
import bp.res.BPResourceJDBCLink.DBStruct;

public class BPJDBCContextBase implements BPJDBCContext
{
	protected ExecutorService m_exec;
	protected ExecutorService m_execback;
	protected volatile AtomicBoolean m_stoprunflag = new AtomicBoolean(false);
	protected volatile WeakReference<BPResourceJDBCLink> m_linkref;
	protected volatile BPEnv m_env;

	public BPJDBCContextBase(BPResourceJDBCLink link)
	{
		m_env = new BPJDBCContextEnv();
		m_linkref = new WeakReference<BPResourceJDBCLink>(link);
		m_exec = Executors.newSingleThreadExecutor(new JDBCThreadFactory(link, m_stoprunflag, m_env));
		m_execback = Executors.newSingleThreadExecutor(new JDBCThreadFactory(link, m_stoprunflag, m_env));
	}

	public BPResourceJDBCLink getJDBCLink()
	{
		WeakReference<BPResourceJDBCLink> linkref = m_linkref;
		if (linkref != null)
		{
			return linkref.get();
		}
		return null;
	}

	public void open()
	{
	}

	public void setEnv(String key, String value)
	{
		m_env.setEnv(key, value);
	}

	public void close()
	{
		m_linkref = null;
		ExecutorService exec = m_exec;
		ExecutorService execback = m_execback;
		m_exec = null;
		m_execback = null;
		if (exec != null)
		{
			exec.submit(() -> (new BPJDBCContextSegs.BPJDBCContextSegDisconnect()).get());
			exec.shutdown();
		}
		if (execback != null)
		{
			execback.submit(() -> (new BPJDBCContextSegs.BPJDBCContextSegDisconnect()).get());
			execback.shutdown();
		}
	}

	public <T> CompletionStage<T> runSegment(Supplier<T> seg)
	{
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<DBStruct> list()
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegListDBStruct(), m_execback);
	}

	public CompletionStage<Void> listColumns(List<String> tablenames, DBStruct dbstruct)
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegListColumns(tablenames, dbstruct), m_execback);
	}

	public CompletionStage<Boolean> connect()
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegConnect(), m_exec);
	}

	public CompletionStage<Boolean> disconnect()
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegDisconnect(), m_exec);
	}

	public CompletionStage<Boolean> commit()
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegCommit(), m_exec);
	}

	public CompletionStage<Boolean> rollback()
	{
		return CompletableFuture.supplyAsync(new BPJDBCContextSegs.BPJDBCContextSegRollback(), m_exec);
	}

	public CompletionStage<Integer> execute(String sql, Object[] params)
	{
		JDBCSeg<Integer> seg = new BPJDBCContextSegs.BPJDBCContextSegExecute(sql, params, null);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<Boolean> interactiveBatchExecute(String sql, BiFunction<Integer, Exception, Object[]> callback)
	{
		JDBCSeg<Boolean> seg = new BPJDBCContextSegs.BPJDBCContextSegInteractiveExecute(sql, callback);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<BPXYDData> startQuery(String sql, Object[] params, Consumer<BPXYDData> preparecallback)
	{
		JDBCSeg<BPXYDData> seg = new BPJDBCContextSegs.BPJDBCContextSegStartQuery(sql, params, null, preparecallback);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<BPXYDData> startQuery(String sql, Object[] params)
	{
		return startQuery(sql, params, null);
	}

	public CompletionStage<BPXYData> query(String sql, Object[] params)
	{
		JDBCSeg<BPXYData> seg = new BPJDBCContextSegs.BPJDBCContextSegQuery(sql, params, null, null);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<BPXYDData> resumeQuery(Consumer<BPXYDData> preparecallback)
	{
		JDBCSeg<BPXYDData> seg = new BPJDBCContextSegs.BPJDBCContextSegResumeQuery(preparecallback);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public CompletionStage<BPXYDData> resumeQueryToEnd(Consumer<BPXYDData> preparecallback)
	{
		JDBCSeg<BPXYDData> seg = new BPJDBCContextSegs.BPJDBCContextSegResumeQueryToEnd(preparecallback);
		return CompletableFuture.supplyAsync(seg, m_exec);
	}

	public void stopRunSQL()
	{
		m_stoprunflag.set(true);
	}

	protected static class JDBCThreadFactory implements ThreadFactory
	{
		protected volatile BPResourceJDBCLink m_link;
		protected WeakReference<AtomicBoolean> m_stoprunflag;
		protected volatile BPEnv m_env;

		public JDBCThreadFactory(BPResourceJDBCLink link, AtomicBoolean stoprunflag, BPEnv env)
		{
			m_link = link;
			m_stoprunflag = new WeakReference<AtomicBoolean>(stoprunflag);
			m_env = env;
		}

		public Thread newThread(Runnable r)
		{
			JDBCThread t = new JDBCThread(m_link, r, m_stoprunflag);
			BPEnv env = m_env;
			if (env != null)
				t.setEnv(env);
			return t;
		}
	}
}
