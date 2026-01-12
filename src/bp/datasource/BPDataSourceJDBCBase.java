package bp.datasource;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import bp.env.BPEnvJDBC;
import bp.jdbc.BPJDBCSyncContext;
import bp.jdbc.BPJDBCSyncContextBase;

public class BPDataSourceJDBCBase implements BPDataSourceJDBC
{
	protected ThreadLocal<BPJDBCSyncContext> m_tlcons = new ThreadLocal<BPJDBCSyncContext>();
	protected Queue<BPJDBCSyncContext> m_pool = new ConcurrentLinkedQueue<BPJDBCSyncContext>();
	protected volatile int m_used;
	protected volatile int m_all;
	protected volatile int m_poolsize = 0;
	protected volatile BPEnvJDBC m_env;

	public void setPoolSize(int poolsize)
	{
		m_poolsize = poolsize;
	}

	public void setEnv(BPEnvJDBC env)
	{
		m_env = env;
	}

	public BPEnvJDBC getEnv()
	{
		return m_env;
	}

	public <R> R use(Function<BPJDBCSyncContext, R> seg)
	{
		R rc = null;
		BPJDBCSyncContext context = m_tlcons.get();
		boolean success = false;
		if (context != null)
		{
			rc = seg.apply(context);
		}
		else
		{
			try
			{
				context = getJDBCContext();
				m_tlcons.set(context);
				rc = seg.apply(context);
				context.commit();
				success = true;
			}
			finally
			{
				m_tlcons.remove();
				if (!success)
				{
					if (context != null)
						context.rollback();
				}
				if (context != null)
					freeContext(context);
			}
		}
		return rc;
	}

	protected void freeContext(BPJDBCSyncContext context)
	{
		if (context.isValid())
		{
			if (m_poolsize <= 0 || m_poolsize > m_pool.size())
				m_pool.add(context);
			else
			{
				m_all--;
				context.close();
			}
			m_used--;
		}
	}

	protected BPJDBCSyncContext getJDBCContext()
	{
		BPJDBCSyncContext rc = m_pool.poll();
		if (rc == null)
			rc = createJDBCContext();
		m_used++;
		return rc;
	}

	protected BPJDBCSyncContext createJDBCContext()
	{
		BPJDBCSyncContextBase rc = new BPJDBCSyncContextBase(m_env.getMappedData());
		m_all++;
		return rc;
	}

	public Map<String, Object> getMappedData()
	{
		Map<String, Object> rc = new HashMap<String, Object>();
		rc.put("used", m_used);
		rc.put("all", m_all);
		rc.put("poolsize", m_poolsize);
		rc.put("poolsizereal", m_pool.size());
		return rc;
	}
}