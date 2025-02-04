package bp.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import bp.BPCore;
import bp.cache.BPCacheDataFileSystem;
import bp.cache.BPCacheJDBC;
import bp.cache.BPCacheJDBCBase;
import bp.cache.BPEventCache;
import bp.cache.BPTreeCacheNode;
import bp.config.BPConfig;
import bp.config.BPConfigJDBC;
import bp.event.BPEventCoreUI;
import bp.format.BPFormatJDBCLink;
import bp.res.BPResource;
import bp.res.BPResourceDir;
import bp.res.BPResourceFile;
import bp.res.BPResourceFileLocal;
import bp.res.BPResourceJDBCLink;
import bp.util.IOUtil;
import bp.util.JSONUtil;
import bp.util.Std;

public class BPResourceProjectJDBC extends BPResourceProjectFile
{
	protected BPConfigJDBC m_config;
	protected volatile BPCacheJDBCBase m_cache = new BPCacheJDBCBase();
	protected Consumer<BPEventCache> m_dbstructcachehandler;
	protected volatile List<BPResourceJDBCLink> m_cachelinks;

	public final static String PRJTYPE_JDBC = "jdbc";

	public BPResourceProjectJDBC(BPResourceDir dir)
	{
		super(dir, false);
		m_config = new BPConfigJDBC();
		m_dbstructcachehandler = this::onDBStructCacheChanged;
		m_cache.setPathKey(m_pathkey);
		BPCore.EVENTS_CACHE.on(m_cache.getEventChannelID(), BPEventCache.EVENTKEY_CACHE_CHANGED, m_dbstructcachehandler);
	}

	public BPCacheJDBC getCache()
	{
		return m_cache;
	}

	public String getResType()
	{
		return "jdbc project";
	}

	public BPConfig getJDBCConfigs()
	{
		return m_config;
	}

	public String getProjectTypeName()
	{
		return PRJTYPE_JDBC;
	}

	public BPProjectItemFactory[] getItemFactories()
	{
		return new BPProjectItemFactory[] { new BPProjectItemJDBCFactory() };
	}

	public List<BPResource> getProjectFunctionItems()
	{
		List<BPResource> rc = new ArrayList<BPResource>();
		List<BPResourceJDBCLink> cachelinks = m_cachelinks;
		if (cachelinks != null)
		{
			rc.addAll(cachelinks);
		}
		return rc;
	}

	public void refreshByCache(BPTreeCacheNode<BPCacheDataFileSystem> root)
	{
		super.refreshByCache(root);
		long ct = System.currentTimeMillis();
		List<BPTreeCacheNode<?>> nodes = new ArrayList<BPTreeCacheNode<?>>();
		root.filter(node -> node.getKey().toLowerCase().endsWith(".bpjdbc"), nodes);
		List<BPResourceJDBCLink> jdbclinks = new ArrayList<BPResourceJDBCLink>();
		for (BPTreeCacheNode<?> node : nodes)
		{
			BPCacheDataFileSystem f = (BPCacheDataFileSystem) node.getValue();
			BPResourceFileLocal fres = new BPResourceFileLocal(f.getFullName());
			try
			{
				jdbclinks.add(BPResourceJDBCLink.readLink(fres));
			}
			catch (Exception e)
			{
				Std.err(e);
			}
		}
		long ct2 = System.currentTimeMillis();
		Std.debug("JDBCLink Cache:" + jdbclinks.size() + " Loaded in " + (ct2 - ct) + "ms");
		m_cachelinks = new CopyOnWriteArrayList<BPResourceJDBCLink>(jdbclinks);
	}

	public static class BPProjectItemJDBCFactory implements BPProjectItemFactory
	{
		public String getName()
		{
			return BPFormatJDBCLink.FORMAT_JDBCLINK;
		}

		public void create(Map<String, Object> params, BPResourceProject project, BPResource par)
		{
			String name = (String) params.get("name");
			BPResourceDir p = (BPResourceDir) par;
			if (p == null)
				p = (BPResourceDir) project;
			BPResourceFile f = (BPResourceFile) p.createChild(name + ".bpjdbc", true);
			BPResourceJDBCLink link = new BPResourceJDBCLink(f);
			link.setMappedData(params);
			writeJDBCLink(f, link);
		}

		public String getItemClassName()
		{
			return BPResourceJDBCLink.class.getName();
		}
	}

	public final static boolean writeJDBCLink(BPResourceFile res, BPResourceJDBCLink link)
	{
		return res.useOutputStream((out) ->
		{
			try
			{
				String str = JSONUtil.encode(link.getMappedData(), 6);
				IOUtil.write(out, str.getBytes("utf-8"));
				return true;
			}
			catch (Exception e)
			{
				Std.err(e);
			}
			return false;
		});
	}

	public BPResource wrapResource(BPResource res)
	{
		BPResource rc = null;
		if (res.isFileSystem())
		{
			if (".bpjdbc".equals(res.getExt()))
			{
				rc = BPResourceJDBCLink.readLink((BPResourceFile) res);
			}
			else if (".bpprj".equalsIgnoreCase(res.getName()))
			{
				return null;
			}
		}
		if (rc == null)
		{
			rc = super.wrapResource(res);
		}
		return rc;
	}

	public void save(BPResource res)
	{
		if (res instanceof BPResourceJDBCLink)
		{
			BPResourceJDBCLink link = (BPResourceJDBCLink) res;
			BPResourceFile raw = (BPResourceFile) link.getRawResource();
			writeJDBCLink(raw, link);
		}
	}

	protected void onDBStructCacheChanged(BPEventCache event)
	{
		if (event.subkey.equals(m_pathkey))
		{
			BPCore.EVENTS_CORE.trigger(BPCore.getCoreUIChannelID(), BPEventCoreUI.refreshProjectTree(m_pathkey, event.datas[0]));
		}
	}
}
