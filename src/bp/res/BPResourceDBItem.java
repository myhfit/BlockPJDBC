package bp.res;

import java.util.ArrayList;
import java.util.List;

import bp.BPCore;
import bp.cache.BPCacheJDBC;
import bp.context.BPProjectsContext;
import bp.project.BPResourceProjectJDBC;
import bp.res.BPResourceJDBCLink.DBColumn;
import bp.res.BPResourceJDBCLink.DBSchema;
import bp.res.BPResourceJDBCLink.DBStruct;
import bp.res.BPResourceJDBCLink.DBTable;

public class BPResourceDBItem extends BPResourceVirtual
{
	public BPResourceDBItem(BPResource par, String name)
	{
		m_parent = par;
		m_name = name;
	}

	public String toString()
	{
		return m_name;
	}

	public BPResourceJDBCLink getJDBCLink()
	{
		BPResource par = m_parent;
		while (par != null)
		{
			if (par instanceof BPResourceJDBCLink)
			{
				return (BPResourceJDBCLink) par;
			}
			par = par.getParentResource();
		}
		return null;
	}

	public static class BPResourceDBSchema extends BPResourceDBItem
	{
		public BPResourceDBSchema(BPResource par, DBSchema schema, DBStruct dbstruct)
		{
			super(par, schema.name);
			List<DBTable> tables = dbstruct.tables.get(schema.name);
			m_children = new ArrayList<BPResource>();
			for (DBTable table : tables)
			{
				BPResourceDBTable chd = new BPResourceDBTable(this, schema, table, dbstruct);
				m_children.add(chd);
			}
		}

		public String toString()
		{
			String name = getName();
			if (name == null || name.length() == 0)
				return "[DEFAULT]";
			return name;
		}
	}

	public static class BPResourceDBTable extends BPResourceDBItem
	{
		protected DBStruct dbstruct;

		public BPResourceDBTable(BPResource par, DBSchema schema, DBTable table, DBStruct dbstruct)
		{
			super(par, table.name);
			this.dbstruct = dbstruct;
		}

		public BPResource[] listResources(boolean isdelta)
		{
			if (m_children != null)
			{
				return m_children.toArray(new BPResource[m_children.size()]);
			}
			else
			{
				String schema = m_parent.getName();
				String table = m_name;
				List<DBColumn> cols = dbstruct.columns.get(((schema.length() == 0) ? table : (schema + "." + table)).toUpperCase());
				if (cols != null)
				{
					m_children = new ArrayList<BPResource>();
					for (DBColumn col : cols)
					{
						BPResourceDBColumn chd = new BPResourceDBColumn(this, col);
						m_children.add(chd);
					}
					return m_children.toArray(new BPResource[m_children.size()]);
				}
				else if (!isdelta)
				{
					BPProjectsContext context = BPCore.getProjectsContext();
					BPResourceProjectJDBC prj = (BPResourceProjectJDBC) context.getRootProject(this);
					BPCacheJDBC cache = prj.getCache();
					cache.addCacheTask(this);
					cache.start();
					return null;
				}
			}
			return null;
		}
	}

	public static class BPResourceDBColumn extends BPResourceDBItem
	{
		public BPResourceDBColumn(BPResource par, DBColumn column)
		{
			super(par, column.name + column.info);
			m_isleaf = true;
		}
	}
}
