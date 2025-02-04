package bp.cache;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import bp.BPCore;
import bp.event.BPEventChannelBase;
import bp.jdbc.BPJDBCContextBase;
import bp.res.BPResourceJDBCLink;
import bp.res.BPResource;
import bp.res.BPResourceDBItem.BPResourceDBSchema;
import bp.res.BPResourceDBItem.BPResourceDBTable;
import bp.res.BPResourceJDBCLink.DBStruct;
import bp.util.Std;

public class BPCacheJDBCBase extends BPCacheBase implements BPCacheJDBC
{
	protected Queue<BPResource> m_cacheq = new ConcurrentLinkedQueue<BPResource>();

	protected Map<String, DBStruct> m_data = new ConcurrentHashMap<String, DBStruct>();

	protected volatile int m_channelid = BPCore.EVENTS_CACHE.addChannel(new BPEventChannelBase());

	protected volatile String m_pathkey;

	protected ExecutorService m_execs = Executors.newSingleThreadExecutor((r) ->
	{
		Thread t = new Thread();
		t.setDaemon(true);
		return t;
	});

	public int getEventChannelID()
	{
		return m_channelid;
	}

	public void clear()
	{
		m_cacheq.clear();
		m_data.clear();
	}

	public void addCacheTask(BPResourceJDBCLink link)
	{
		if (!m_cacheq.contains(link))
			m_cacheq.add(link);
	}

	public void addCacheTask(BPResourceDBTable table)
	{
		if (!m_cacheq.contains(table))
			m_cacheq.add(table);
	}

	public void setPathKey(String pathkey)
	{
		m_pathkey = pathkey;
	}

	protected boolean doCache()
	{
		Queue<BPResource> ts = m_cacheq;
		BPResource t = ts.poll();
		if (t != null)
		{
			if (t instanceof BPResourceJDBCLink)
			{
				long ct = System.currentTimeMillis();
				BPResourceJDBCLink jdbclink = (BPResourceJDBCLink) t;
				BPJDBCContextBase context = new BPJDBCContextBase(jdbclink);
				try
				{
					DBStruct ds = context.list().toCompletableFuture().get();
					m_data.put(jdbclink.getCacheKey(), ds);
					long ct2 = System.currentTimeMillis();
					Std.debug("DBStruct Cache Loaded in " + (ct2 - ct) + "ms");
					BPCore.EVENTS_CACHE.trigger(m_channelid, new BPEventCache(m_pathkey, t));
				}
				catch (InterruptedException | ExecutionException e)
				{
					Std.err(e);
				}
			}
			else
			{
				long ct = System.currentTimeMillis();
				BPResourceDBTable table = (BPResourceDBTable) t;
				BPResourceDBSchema schema = (BPResourceDBSchema) table.getParentResource();
				BPResourceJDBCLink jdbclink = table.getJDBCLink();
				BPJDBCContextBase context = new BPJDBCContextBase(jdbclink);
				try
				{
					List<String> tablename = new CopyOnWriteArrayList<String>(new String[] { schema.getName().length() == 0 ? table.getName() : schema.getName() + "." + table.getName() });
					context.listColumns(tablename, m_data.get(jdbclink.getCacheKey())).toCompletableFuture().get();
					long ct2 = System.currentTimeMillis();
					Std.debug("DBColumn Cache Loaded in " + (ct2 - ct) + "ms");
					BPCore.EVENTS_CACHE.trigger(m_channelid, new BPEventCache(m_pathkey, t));
				}
				catch (InterruptedException | ExecutionException e)
				{
					Std.err(e);
				}
			}
		}
		return ts.isEmpty();
	}

	public DBStruct getDBStruct(BPResourceJDBCLink link)
	{
		String key = link.getCacheKey();
		return m_data.get(key);
	}
}
