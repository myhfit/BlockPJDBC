package bp.res;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import bp.BPCore;
import bp.cache.BPCacheJDBC;
import bp.context.BPProjectsContext;
import bp.project.BPResourceProjectJDBC;
import bp.res.BPResourceDBItem.BPResourceDBSchema;
import bp.util.IOUtil;
import bp.util.JSONUtil;
import bp.util.Std;
import bp.util.TextUtil;

public class BPResourceJDBCLink extends BPResourceOverlay
{
	protected volatile String m_name;
	protected volatile String m_driver;
	protected volatile String m_url;
	protected volatile String m_user;
	protected volatile String m_password;

	public final static String RESTYPE_JDBCLINK = "jdbclink";

	public BPResourceJDBCLink(BPResourceFile fc)
	{
		super(fc);
	}

	public final static BPResourceJDBCLink readLink(BPResourceFile res)
	{
		BPResourceJDBCLink rc = null;
		Map<String, Object> params = res.useInputStream((in) ->
		{
			try
			{
				String str = TextUtil.toString(IOUtil.read(in), "utf-8");
				return JSONUtil.decode(str);
			}
			catch (Exception e)
			{
				Std.err(e);
			}
			return null;
		});
		if (params != null)
		{
			rc = new BPResourceJDBCLink(res);
			rc.setMappedData(params);
		}
		return rc;
	}

	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other instanceof BPResourceJDBCLink)
		{
			return m_res.equals(((BPResourceJDBCLink) other).getRawResource());
		}
		return false;
	}

	public String toString()
	{
		return m_name;
	}

	public Map<String, Object> getMappedData()
	{
		Map<String, Object> rc = new HashMap<String, Object>();
		rc.put("name", m_name);
		rc.put("driver", m_driver);
		rc.put("url", m_url);
		rc.put("user", m_user);
		rc.put("password", m_password);
		return rc;
	}

	public void setMappedData(Map<String, Object> data)
	{
		m_name = (String) data.get("name");
		m_driver = (String) data.get("driver");
		m_url = (String) data.get("url");
		m_user = (String) data.get("user");
		m_password = (String) data.get("password");
	}

	public String getDriver()
	{
		return m_driver;
	}

	public String getURL()
	{
		return m_url;
	}

	public String getResType()
	{
		return RESTYPE_JDBCLINK;
	}

	public boolean isLeaf()
	{
		return false;
	}

	public BPResource[] listResources(boolean isdelta)
	{
		BPProjectsContext context = (BPProjectsContext) BPCore.getFileContext();
		BPResourceProjectJDBC prj = (BPResourceProjectJDBC) context.getRootProject(this);
		BPCacheJDBC cache = prj.getCache();
		DBStruct ds = cache.getDBStruct(this);
		if (ds == null && !isdelta)
		{
			cache.addCacheTask(this);
			cache.start();
		}
		if (ds != null)
		{
			List<BPResource> chds = new ArrayList<BPResource>();
			for (DBSchema schema : ds.schemas)
			{
				BPResourceDBSchema chd = new BPResourceDBSchema(this, schema, ds);
				chds.add(chd);
			}
			return chds.toArray(new BPResource[chds.size()]);
		}
		return null;
	}

	public String getCacheKey()
	{
		return m_url + "+" + m_user;
	}

	public String getName()
	{
		return m_name;
	}

	public boolean isFileSystem()
	{
		return false;
	}

	public boolean isProjectResource()
	{
		return true;
	}

	public boolean fullHandleAction()
	{
		return true;
	}

	public boolean rename(String newname)
	{
		return false;
	}

	public static class DBSchema implements DBStructItem
	{
		public String name;

		public String getSQLText()
		{
			return "\"" + name + "\"";
		}

		public String getName()
		{
			return name;
		}

		public DBStructItemType getItemType()
		{
			return DBStructItemType.SCHEMA;
		}

		public DBSchema(String _name)
		{
			name = _name;
		}

		public DBSchema()
		{

		}

		public String toString()
		{
			if (name == null || name.length() == 0)
				return "[DEFAULT]";
			return name;
		}
	}

	@SuppressWarnings("serial")
	public static class DBColumnCache extends CopyOnWriteArrayList<DBColumn>
	{
		public AtomicBoolean cached = new AtomicBoolean(false);
	}

	public enum DBTableTypes
	{
		TABLE, VIEW
	}

	public enum DBStructItemType
	{
		SCHEMA(false), TABLE(true), COLUMN(true);

		private boolean m_isleaf;

		private DBStructItemType(boolean isleaf)
		{
			m_isleaf = isleaf;
		}

		public boolean isLeaf()
		{
			return m_isleaf;
		}
	}

	public static interface DBStructItem
	{
		public String getSQLText();

		public DBStructItemType getItemType();

		public String getName();
	}

	public static class DBTable implements DBStructItem
	{
		public DBTableTypes dstype;
		public String name;

		public String getSQLText()
		{
			return "\"" + name + "\"";
		}

		public String getName()
		{
			return name;
		}

		public DBStructItemType getItemType()
		{
			return DBStructItemType.TABLE;
		}

		public String toString()
		{
			return name;
		}
	}

	public static class DBColumn implements DBStructItem
	{
		public String info;
		public String name;
		public String tablename;

		public String getSQLText()
		{
			return "\"" + name + "\"";
		}

		public String getName()
		{
			return name;
		}

		public DBStructItemType getItemType()
		{
			return DBStructItemType.COLUMN;
		}

		public String toString()
		{
			return tablename + "." + name;
		}
	}

	public static class DBStruct
	{
		public List<DBSchema> schemas;
		public Map<String, List<DBTable>> tables;
		public Map<String, DBColumnCache> columns;

		public List<DBColumn> findColumn(String schema, List<String> tablenames, String columnname)
		{
			List<DBColumn> rc = new ArrayList<DBColumn>();
			if (columns != null)
			{
				for (String tablename : tablenames)
				{
					String tname;
					int vi = tablename.indexOf(".");
					if (vi > -1)
					{
						tname = tablename;
					}
					else
					{
						tname = ((schema == null || schema.length() == 0) ? tablename : schema + "." + tablename);
					}
					List<DBColumn> cols = columns.get(tname.toUpperCase());
					if (cols != null)
					{
						if (columnname != null)
						{
							for (DBColumn col : cols)
							{
								if (col.name.toUpperCase().contains(columnname.toUpperCase()))
								{
									rc.add(col);
								}
							}
						}
						else
							rc.addAll(cols);
					}
				}
			}
			return rc;
		}

		public List<String> presetColumnCache(String schema, List<String> tablenames)
		{
			List<String> rc = new ArrayList<String>();
			if (columns != null)
			{
				for (String tablename : tablenames)
				{
					String tname;
					int vi = tablename.indexOf(".");
					if (vi > -1)
					{
						tname = tablename;
					}
					else
					{
						tname = ((schema == null || schema.length() == 0) ? tablename : schema + "." + tablename);
					}
					if (columns.containsKey(tname.toUpperCase()))
					{
						DBColumnCache cols = columns.get(tname.toUpperCase());
						if (cols.cached.compareAndSet(false, true))
						{
							rc.add(tname);
						}
					}
				}
			}
			return rc;
		}

		public List<DBTable> findTable(String schema, String key)
		{
			List<DBTable> rc = null;
			if (schema == null)
				schema = "";
			if (tables != null)
			{
				List<DBTable> ts = tables.get(schema);
				if (key == null || key.trim().length() == 0)
				{
					if (ts == null)
						rc = new ArrayList<DBTable>();
					else
						rc = new ArrayList<DBTable>(ts);
				}
				else
				{
					key = key.trim().toUpperCase();
					rc = new ArrayList<DBTable>();
					for (DBTable t : ts)
					{
						if (t.name.toUpperCase().startsWith(key))
							rc.add(t);
					}
					for (DBTable t : ts)
					{
						int vi = t.name.toUpperCase().indexOf(key);
						if (vi > 0)
							rc.add(t);
					}
				}
			}
			if (rc == null)
				rc = new ArrayList<DBTable>();
			return rc;
		}

		public List<DBSchema> findSchema(String schema)
		{
			List<DBSchema> rc = new ArrayList<DBSchema>();
			if (schemas != null)
			{
				for (DBSchema s : schemas)
				{
					if (s.getName().length() > 0 && (schema == null || schema.length() == 0 || s.getName().toUpperCase().indexOf(schema.toUpperCase()) > -1))
						rc.add(s);
				}
			}
			return rc;
		}

		public List<Object> getRoots()
		{
			return schemas != null ? new ArrayList<Object>(schemas) : new ArrayList<Object>();
		}

		public List<Object> getChildren(Object node)
		{
			if (node instanceof DBStructItem)
			{
				if (((DBStructItem) node).getItemType() == DBStructItemType.SCHEMA)
				{
					List<Object> rc = new ArrayList<Object>();
					rc.addAll(findTable(((DBSchema) node).getName(), ""));
					return rc;
				}
			}
			return null;
		}

		public boolean isLeaf(Object node)
		{
			if (node == this)
				return false;
			if (node instanceof DBStructItem)
			{
				DBStructItem item = (DBStructItem) node;
				return item.getItemType().isLeaf();
			}
			return true;
		}
	}

}
