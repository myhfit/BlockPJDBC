package bp.jdbc;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import bp.data.BPXData;
import bp.data.BPXYDData;
import bp.data.BPXYData;
import bp.jdbc.BPJDBCContextBase.JDBCSeg;
import bp.jdbc.BPJDBCContextBase.JDBCThread;
import bp.res.BPResourceJDBCLink;
import bp.res.BPResourceJDBCLink.DBColumn;
import bp.res.BPResourceJDBCLink.DBColumnCache;
import bp.res.BPResourceJDBCLink.DBSchema;
import bp.res.BPResourceJDBCLink.DBStruct;
import bp.res.BPResourceJDBCLink.DBTable;
import bp.res.BPResourceJDBCLink.DBTableTypes;
import bp.util.ObjUtil;
import bp.util.Std;

public class BPJDBCContextSegs
{
	public final static class BPJDBCContextSegChangeJDBCLink extends JDBCSeg<Void>
	{
		protected volatile BPResourceJDBCLink m_link;

		public BPJDBCContextSegChangeJDBCLink(BPResourceJDBCLink link)
		{
			m_link = link;
		}

		public Void get()
		{
			getThread().setJDBCLink(m_link);
			return null;
		}
	}

	public final static class BPJDBCContextSegListColumns extends BPJDBCContextSegRun<Void>
	{
		protected volatile List<String> m_tablenames;
		protected volatile DBStruct m_dbstruct;

		public BPJDBCContextSegListColumns(List<String> tablenames, DBStruct dbstruct)
		{
			m_tablenames = tablenames;
			m_dbstruct = dbstruct;
		}

		public Void get()
		{
			JDBCThread thread = getThread();
			connect(thread);
			Connection conn = getConnection();
			ResultSet rs = null;
			List<String> tablenames = m_tablenames;
			DBStruct dbstruct = m_dbstruct;
			try
			{
				for (String tablename : tablenames)
				{
					int vi = tablename.indexOf('.');
					String tn;
					String schema;
					if (vi > -1)
					{
						tn = tablename.substring(vi + 1);
						schema = tablename.substring(0, vi);
					}
					else
					{
						tn = tablename;
						schema = "";
					}
					rs = conn.getMetaData().getColumns(null, schema, tn, "%");
					ResultSetMetaData md = rs.getMetaData();
					BPJDBCQueryResult result = BPJDBCQueryResult.createByMetaData(md);
					if (result.read(rs, 0, 0))
					{
						dbstruct.columns.put(((schema.length() == 0) ? tn : (schema + "." + tn)).toUpperCase(), new DBColumnCache());
						List<BPXData> datas = result.getDatas();
						String[] colnames = result.getColumnNames();
						Map<String, Integer> colmap = new HashMap<String, Integer>();
						for (int i = 0; i < colnames.length; i++)
						{
							colmap.put(colnames[i].toUpperCase(), i);
						}
						List<DBColumn> cols = new ArrayList<DBColumn>();
						for (BPXData data : datas)
						{
							DBColumn col = new DBColumn();
							col.name = (String) data.getColValue(colmap.get("COLUMN_NAME"));
							String ctype = (String) data.getColValue(colmap.get("TYPE_NAME"));
							Number nlen = ((Number) data.getColValue(colmap.get("COLUMN_SIZE")));
							{
								if (nlen != null && nlen.intValue() == Integer.MAX_VALUE)
									nlen = null;
							}
							int nplen = ObjUtil.toInt(data.getColValue(colmap.get("DECIMAL_DIGITS")), -1);
							String nlenstr = "";
							if (nlen != null)
								nlenstr = "(" + nlen + (nplen > 0 ? ("," + nplen) : "") + ")";
							col.info = "(" + ctype + (ctype.contains("(") ? "" : nlenstr) + ")";
							cols.add(col);
						}
						dbstruct.columns.get(tablename.toUpperCase()).addAll(cols);
					}
				}
			}
			catch (SQLException e)
			{
				Std.err(e);
			}
			finally
			{
				if (rs != null)
				{
					try
					{
						rs.close();
					}
					catch (SQLException e)
					{
						Std.err(e);
					}
				}
				disconnect(thread);
			}
			return null;
		}
	}

	public final static class BPJDBCContextSegListDBStruct extends BPJDBCContextSegRun<DBStruct>
	{
		public BPJDBCContextSegListDBStruct()
		{
		}

		public DBStruct get()
		{
			JDBCThread thread = getThread();
			connect(thread);
			Connection conn = getConnection();
			ResultSet rs = null;
			try
			{
				rs = conn.getMetaData().getTables(null, null, "%", new String[] { "TABLE", "VIEW" });
				ResultSetMetaData md = rs.getMetaData();
				BPJDBCQueryResult result = BPJDBCQueryResult.createByMetaData(md);
				if (result.read(rs, 0, 0))
				{
					List<BPXData> datas = result.getDatas();
					return makeDBStruct(result.getColumnNames(), datas);
				}
			}
			catch (SQLException e)
			{
				Std.err(e);
			}
			finally
			{
				if (rs != null)
				{
					try
					{
						rs.close();
					}
					catch (SQLException e)
					{
						Std.err(e);
					}
				}
				disconnect(thread);
			}
			return null;
		}

		protected DBStruct makeDBStruct(String[] colnames, List<BPXData> datas)
		{
			DBStruct dbs = new DBStruct();
			if (colnames != null && datas != null)
			{
				List<DBSchema> schemas = new ArrayList<DBSchema>();
				List<String> schemanames = new ArrayList<String>();
				Map<String, List<DBTable>> tables = new ConcurrentHashMap<String, List<DBTable>>();
				Map<String, DBColumnCache> columns = new ConcurrentHashMap<String, DBColumnCache>();
				int tncol = 0, sccol = 0, ttcol = 0;
				for (int i = 0; i < colnames.length; i++)
				{
					String cn = colnames[i].toUpperCase();
					if (cn.equals("TABLE_NAME"))
					{
						tncol = i;
					}
					else if (colnames[i].equalsIgnoreCase("TABLE_TYPE"))
					{
						ttcol = i;
					}
					else if (cn.equals("TABLE_SCHEM"))
					{
						sccol = i;
					}
				}
				for (BPXData datax : datas)
				{
					Object[] data = datax.getValues();
					Object schemao = data[sccol];
					String schemaname = schemao != null ? schemao.toString() : "";
					Object tnameo = data[tncol];
					String tname = tnameo != null ? tnameo.toString() : "";
					Object ttypeo = data[ttcol];
					String ttype = ttypeo != null ? ttypeo.toString() : "";
					List<DBTable> dts;
					if (!schemanames.contains(schemaname))
					{
						schemas.add(new DBSchema(schemaname));
						schemanames.add(schemaname);
						dts = new ArrayList<DBTable>();
						tables.put(schemaname, dts);
					}
					else
					{
						dts = tables.get(schemaname);
					}
					DBTable dt = new DBTable();
					dt.name = tname;
					if (ttype.equalsIgnoreCase("TABLE"))
						dt.dstype = DBTableTypes.TABLE;
					else if (ttype.equalsIgnoreCase("VIEW"))
						dt.dstype = DBTableTypes.VIEW;
					dts.add(dt);
				}
				dbs.schemas = new CopyOnWriteArrayList<DBSchema>(schemas);
				dbs.tables = tables;
				dbs.columns = columns;
			}
			return dbs;
		}
	}

	public static abstract class BPJDBCContextSegRun<V> extends JDBCSeg<V>
	{
		protected final static void stopQuery(JDBCThread thread)
		{
			if (thread.getConnection() != null)
			{
				ResultSet rs = thread.getResultSet();
				Statement st = thread.getStatement();
				stopQuery(rs, st);
				thread.setResultSet(null);
				thread.setStatement(null);
			}
		}

		protected final static void stopQuery(ResultSet rs, Statement st)
		{
			if (rs != null)
			{
				try
				{
					rs.close();
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
				finally
				{
				}
			}
			if (st != null)
			{
				try
				{
					st.close();
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
				finally
				{
				}
			}
		}

		protected final static void tryAutoRollback(JDBCThread thread)
		{
			Connection conn = thread.getConnection();
			if (conn != null)
			{
				boolean needcommit = thread.needCommit();
				try
				{
					if (needcommit)
					{
						try
						{
							conn.rollback();
						}
						finally
						{
							thread.setNeedCommit(false);
						}
					}
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
			}
		}

		protected final static Boolean connect(JDBCThread thread)
		{
			disconnect(thread);
			BPResourceJDBCLink link = thread.getJDBCLink();
			Map<String, Object> linkmap = link.getMappedData();
			String url = (String) linkmap.get("url");
			String user = (String) linkmap.get("user");
			String pass = (String) linkmap.get("password");
			try
			{
				Connection conn = DriverManager.getConnection(url, user, pass);
				conn.setAutoCommit(false);
				thread.setConnection(conn);
				return true;
			}
			catch (SQLException e)
			{
				Std.err(e);
				throw new RuntimeException(e);
			}
		}

		protected final static Boolean disconnect(JDBCThread thread)
		{
			stopQuery(thread);
			tryAutoRollback(thread);
			Connection conn = thread.getConnection();
			if (conn != null)
			{
				try
				{
					conn.close();
					return true;
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
				finally
				{
					thread.setConnection(null);
				}
				return null;
			}
			else
			{
				return false;
			}
		}

		protected final static Boolean commit(JDBCThread thread)
		{
			stopQuery(thread);
			Connection conn = thread.getConnection();
			if (conn != null)
			{
				try
				{
					conn.commit();
					thread.setNeedCommit(false);
					return true;
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
				finally
				{
				}
				return null;
			}
			else
			{
				return false;
			}
		}

		protected final static Boolean rollback(JDBCThread thread)
		{
			stopQuery(thread);
			Connection conn = thread.getConnection();
			if (conn != null)
			{
				try
				{
					conn.rollback();
					thread.setNeedCommit(false);
					return true;
				}
				catch (SQLException e)
				{
					Std.err(e);
				}
				finally
				{
				}
				return null;
			}
			else
			{
				return false;
			}
		}
	}

	public final static class BPJDBCContextSegConnect extends BPJDBCContextSegRun<Boolean>
	{
		public Boolean get()
		{
			return connect(getThread());
		}
	}

	public final static class BPJDBCContextSegDisconnect extends BPJDBCContextSegRun<Boolean>
	{
		public Boolean get()
		{
			return disconnect(getThread());
		}
	}

	public final static class BPJDBCContextSegCommit extends BPJDBCContextSegRun<Boolean>
	{
		public Boolean get()
		{
			return commit(getThread());
		}
	}

	public final static class BPJDBCContextSegRollback extends BPJDBCContextSegRun<Boolean>
	{
		public Boolean get()
		{
			return rollback(getThread());
		}
	}

	public static class BPJDBCContextSegStartQuery extends BPJDBCContextSegRun<BPXYDData>
	{
		protected volatile String m_sql;
		protected volatile Object[] m_params;
		protected volatile Map<String, Object> m_options;
		protected volatile WeakReference<Consumer<BPXYDData>> m_pcallback;

		public BPJDBCContextSegStartQuery(String sql, Object[] params, Map<String, Object> options, Consumer<BPXYDData> preparecallback)
		{
			m_sql = sql;
			m_params = params;
			m_options = options;
			m_pcallback = new WeakReference<Consumer<BPXYDData>>(preparecallback);
		}

		public BPXYDData get()
		{
			JDBCThread thread = getThread();
			stopQuery(thread);
			Connection conn = thread.getConnection();
			BPJDBCQueryResult result = null;
			if (conn != null)
			{
				String sql = m_sql;
				Object[] params = m_params;
				// Map<String, Object> options = m_options;
				Statement st = null;
				ResultSet rs = null;
				thread.setStartRun();
				try
				{
					if (params != null && params.length > 0)
					{
						PreparedStatement ps = conn.prepareStatement(sql);
						st = ps;
						ps.setFetchSize(100);
						try
						{
							ps.setFetchDirection(ResultSet.FETCH_FORWARD);
						}
						catch (SQLException eno)
						{
						}
						if (params != null && params.length > 0)
						{
							for (int i = 0; i < params.length; i++)
							{
								ps.setObject(i + 1, params[i]);
							}
						}
						rs = ps.executeQuery();
					}
					else
					{
						st = conn.createStatement();
						st.setFetchSize(100);
						try
						{
							st.setFetchDirection(ResultSet.FETCH_FORWARD);
						}
						catch (SQLException eno)
						{
						}
						rs = st.executeQuery(sql);
					}
					ResultSetMetaData md = rs.getMetaData();
					result = BPJDBCQueryResult.createByMetaData(md);
					WeakReference<Consumer<BPXYDData>> pcallbackref = m_pcallback;
					m_pcallback = null;
					if (pcallbackref != null)
					{
						Consumer<BPXYDData> pcallback = pcallbackref.get();
						if (pcallback != null)
							pcallback.accept(result);
					}
					result.read(rs, 100, 100);

					thread.setResultSet(rs);
					thread.setStatement(st);
				}
				catch (SQLException e)
				{
					stopQuery(rs, st);
					rs = null;
					st = null;
					throw new RuntimeException(e);
				}
				catch (RuntimeException | ThreadDeath e)
				{
					throw e;
				}
				catch (Exception e)
				{
					Std.err(e);
					stopQuery(rs, st);
					rs = null;
					st = null;
					throw new RuntimeException(e);
				}
				finally
				{

				}
			}
			return result;
		}
	}

	public static class BPJDBCContextSegQuery extends BPJDBCContextSegRun<BPXYData>
	{
		protected volatile String m_sql;
		protected volatile Object[] m_params;
		protected volatile Map<String, Object> m_options;
		protected volatile WeakReference<Consumer<BPXYDData>> m_pcallback;

		public BPJDBCContextSegQuery(String sql, Object[] params, Map<String, Object> options, Consumer<BPXYDData> preparecallback)
		{
			m_sql = sql;
			m_params = params;
			m_options = options;
			m_pcallback = new WeakReference<Consumer<BPXYDData>>(preparecallback);
		}

		public BPXYData get()
		{
			JDBCThread thread = getThread();
			stopQuery(thread);
			Connection conn = thread.getConnection();
			BPJDBCQueryResult result = null;
			if (conn != null)
			{
				String sql = m_sql;
				Object[] params = m_params;
				// Map<String, Object> options = m_options;
				Statement st = null;
				ResultSet rs = null;
				thread.setStartRun();
				try
				{
					if (params != null && params.length > 0)
					{
						PreparedStatement ps = conn.prepareStatement(sql);
						st = ps;
						ps.setFetchSize(100);
						try
						{
							ps.setFetchDirection(ResultSet.FETCH_FORWARD);
						}
						catch (SQLException eno)
						{
						}
						if (params != null && params.length > 0)
						{
							for (int i = 0; i < params.length; i++)
							{
								ps.setObject(i + 1, params[i]);
							}
						}
						rs = ps.executeQuery();
					}
					else
					{
						st = conn.createStatement();
						st.setFetchSize(100);
						try
						{
							st.setFetchDirection(ResultSet.FETCH_FORWARD);
						}
						catch (SQLException eno)
						{
						}
						rs = st.executeQuery(sql);
					}
					ResultSetMetaData md = rs.getMetaData();
					result = BPJDBCQueryResult.createByMetaData(md);
					WeakReference<Consumer<BPXYDData>> pcallbackref = m_pcallback;
					m_pcallback = null;
					if (pcallbackref != null)
					{
						Consumer<BPXYDData> pcallback = pcallbackref.get();
						if (pcallback != null)
							pcallback.accept(result);
					}
					result.read(rs, 0, 0);

					thread.setResultSet(rs);
					thread.setStatement(st);
				}
				catch (SQLException e)
				{
					stopQuery(rs, st);
					rs = null;
					st = null;
					throw new RuntimeException(e);
				}
				catch (RuntimeException | ThreadDeath e)
				{
					throw e;
				}
				catch (Exception e)
				{
					Std.err(e);
					stopQuery(rs, st);
					rs = null;
					st = null;
					throw new RuntimeException(e);
				}
				finally
				{

				}
			}
			return result;
		}
	}

	public final static class BPJDBCContextSegInteractiveExecute extends BPJDBCContextSegRun<Boolean>
	{
		protected volatile WeakReference<BiFunction<Integer, Exception, Object[]>> m_cbref;
		protected volatile String m_sql;

		public BPJDBCContextSegInteractiveExecute(String sql, BiFunction<Integer, Exception, Object[]> callback)
		{
			m_sql = sql;
			m_cbref = new WeakReference<BiFunction<Integer, Exception, Object[]>>(callback);
		}

		public Boolean get()
		{
			String sql = m_sql;
			BiFunction<Integer, Exception, Object[]> cb = m_cbref.get();
			JDBCThread thread = getThread();
			Connection conn = thread.getConnection();
			int batch = 1000;
			int c = 0;
			try
			{
				PreparedStatement p = conn.prepareStatement(sql);
				Object[] params = cb.apply(null, null);
				while (params != null)
				{
					int l = params.length;
					for (int i = 0; i < l; i++)
					{
						p.setObject(i + 1, params[i]);
					}
					p.addBatch();
					if (c < batch)
					{
						c++;
						params = cb.apply(1, null);
					}
					else
					{
						p.executeBatch();
						conn.commit();
						params = cb.apply(1, null);
						c = 0;
					}

				}
				if (c > 0)
				{
					p.executeBatch();
					conn.commit();
				}
				return true;
			}
			catch (SQLException e)
			{
				Std.err(e);
			}
			return false;
		}
	}

	public final static class BPJDBCContextSegExecute extends BPJDBCContextSegRun<Integer>
	{
		protected volatile String m_sql;
		protected volatile Object[] m_params;
		protected volatile Map<String, Object> m_options;

		public BPJDBCContextSegExecute(String sql, Object[] params, Map<String, Object> options)
		{
			m_sql = sql;
			m_params = params;
			m_options = options;
		}

		public Integer get()
		{
			JDBCThread thread = getThread();
			if (thread != null)
			{
				stopQuery(thread);
				Connection conn = thread.getConnection();
				if (conn != null)
				{
					String sql = m_sql;
					Object[] params = m_params;
					// Map<String, Object> options = m_options;
					Statement st = null;

					try
					{
						if (params != null && params.length > 0)
						{
							PreparedStatement ps = conn.prepareStatement(sql);
							st = ps;
							if (params != null && params.length > 0)
							{
								for (int i = 0; i < params.length; i++)
								{
									ps.setObject(i + 1, params[i]);
								}
							}
							thread.setStatement(st);
							return ps.executeUpdate();
						}
						else
						{
							st = conn.createStatement();
							thread.setStatement(st);
							return st.executeUpdate(sql);
						}
					}
					catch (SQLException e)
					{
						Std.err(e);
						if (st != null)
						{
							try
							{
								st.close();
							}
							catch (SQLException e2)
							{

							}
							st = null;
						}
						thread.setStatement(st);
						throw new RuntimeException(e);
					}
				}
			}
			return null;
		}
	}

	public static class BPJDBCContextSegResumeQuery extends BPJDBCContextSegRun<BPXYDData>
	{
		protected volatile WeakReference<Consumer<BPXYDData>> m_pcallback;

		public BPJDBCContextSegResumeQuery(Consumer<BPXYDData> preparecallback)
		{
			m_pcallback = new WeakReference<Consumer<BPXYDData>>(preparecallback);
		}

		public BPXYDData get()
		{
			BPJDBCQueryResult result = null;
			JDBCThread thread = getThread();
			ResultSet rs = thread.getResultSet();
			if (rs != null)
			{
				try
				{
					ResultSetMetaData md = rs.getMetaData();
					result = BPJDBCQueryResult.createByMetaData(md);
					WeakReference<Consumer<BPXYDData>> pcallbackref = m_pcallback;
					m_pcallback = null;
					if (pcallbackref != null)
					{
						Consumer<BPXYDData> pcallback = pcallbackref.get();
						if (pcallback != null)
							pcallback.accept(result);
					}
					result.read(rs, 100, 100);
				}
				catch (SQLException e)
				{
					Std.err(e);
					throw new RuntimeException(e);
				}
			}
			return result;
		}
	}

	public static class BPJDBCContextSegResumeQueryToEnd extends BPJDBCContextSegRun<BPXYDData>
	{
		protected volatile WeakReference<Consumer<BPXYDData>> m_pcallback;

		public BPJDBCContextSegResumeQueryToEnd(Consumer<BPXYDData> preparecallback)
		{
			m_pcallback = new WeakReference<Consumer<BPXYDData>>(preparecallback);
		}

		public BPXYDData get()
		{
			BPJDBCQueryResult result = null;
			JDBCThread thread = getThread();
			ResultSet rs = thread.getResultSet();
			if (rs != null)
			{
				try
				{
					ResultSetMetaData md = rs.getMetaData();
					result = BPJDBCQueryResult.createByMetaData(md);
					WeakReference<Consumer<BPXYDData>> pcallbackref = m_pcallback;
					m_pcallback = null;
					if (pcallbackref != null)
					{
						Consumer<BPXYDData> pcallback = pcallbackref.get();
						if (pcallback != null)
							pcallback.accept(result);
					}
					result.read(rs, 0, 100, () -> thread.getStopRunFlag());
				}
				catch (SQLException e)
				{
					Std.err(e);
					throw new RuntimeException(e);
				}
			}
			return result;
		}
	}
}
