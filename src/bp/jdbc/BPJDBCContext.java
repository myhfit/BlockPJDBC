package bp.jdbc;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import bp.data.BPXYDData;
import bp.data.BPXYData;
import bp.env.BPEnv;
import bp.env.BPEnvDynamic;
import bp.res.BPResourceJDBCLink;
import bp.util.ClassUtil;

public interface BPJDBCContext extends Closeable
{
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

	<T> CompletionStage<T> runSegment(Supplier<T> seg);

	void stopRunSQL();

	void open();

	void close();

	BPResourceJDBCLink getJDBCLink();
	
	public static class BPJDBCContextEnv extends BPEnvDynamic
	{
		public final static String KEY_QUERY_AUTOSTOP = "Q_AUTOSTOP";

		public String getName()
		{
			return "jdbccontext";
		}

		public BPJDBCContextEnv()
		{
			m_rawkeys.add(KEY_QUERY_AUTOSTOP);
		}
	}

	public static class JDBCThread extends Thread
	{
		protected volatile BPResourceJDBCLink m_link;
		protected Connection m_conn;
		protected boolean m_needcommit;
		protected ResultSet m_rs;
		protected Statement m_st;
		protected WeakReference<AtomicBoolean> m_stoprunflag;
		protected volatile BPEnv m_env;

		public JDBCThread(BPResourceJDBCLink link, Runnable r, WeakReference<AtomicBoolean> stoprunflag)
		{
			super(r);
			m_link = link;
			m_stoprunflag = stoprunflag;
			setDaemon(true);
			setContextClassLoader(ClassUtil.getExtensionClassLoader());
		}

		public void setEnv(String key, String value)
		{
			if (m_env == null)
				m_env = new BPJDBCContextEnv();
			m_env.setEnv(key, value);
		}

		public void setEnv(BPEnv env)
		{
			if (m_env == null)
				m_env = new BPJDBCContextEnv();
			m_env.setMappedData(env.getMappedData());
		}

		public String getEnv(String key)
		{
			return m_env == null ? null : m_env.getValue(key);
		}

		public void setStartRun()
		{
			WeakReference<AtomicBoolean> stoprunflagref = m_stoprunflag;
			if (stoprunflagref != null)
			{
				AtomicBoolean stoprunflag = stoprunflagref.get();
				if (stoprunflag != null)
				{
					stoprunflag.set(false);
				}
			}
		}

		public boolean getStopRunFlag()
		{
			boolean rc = false;
			WeakReference<AtomicBoolean> stoprunflagref = m_stoprunflag;
			if (stoprunflagref != null)
			{
				AtomicBoolean stoprunflag = stoprunflagref.get();
				if (stoprunflag != null)
				{
					rc = stoprunflag.get();
				}
			}
			return rc;
		}

		public Connection getConnection()
		{
			return m_conn;
		}

		public boolean needCommit()
		{
			return m_needcommit;
		}

		public void setNeedCommit(boolean flag)
		{
			m_needcommit = flag;
		}

		public BPResourceJDBCLink getJDBCLink()
		{
			return m_link;
		}

		public void setJDBCLink(BPResourceJDBCLink link)
		{
			m_link = link;
		}

		public void setConnection(Connection conn)
		{
			m_conn = conn;
		}

		public ResultSet getResultSet()
		{
			return m_rs;
		}

		public void setResultSet(ResultSet rs)
		{
			m_rs = rs;
		}

		public Statement getStatement()
		{
			return m_st;
		}

		public void setStatement(Statement st)
		{
			m_st = st;
		}
	}

	abstract static class JDBCSeg<V> implements Supplier<V>
	{
		protected JDBCThread getThread()
		{
			return (JDBCThread) Thread.currentThread();
		}

		protected Connection getConnection()
		{
			return getThread().getConnection();
		}
	}
}
