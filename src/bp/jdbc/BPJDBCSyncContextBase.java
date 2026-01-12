package bp.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import bp.data.BPXYData;
import bp.res.BPResourceJDBCLink;
import bp.util.ObjUtil;
import bp.util.Std;

public class BPJDBCSyncContextBase implements BPJDBCSyncContext
{
	protected volatile Map<String, Object> m_linkdata;
	protected volatile Connection m_conn;
	protected volatile boolean m_bcheck;

	public BPJDBCSyncContextBase(BPResourceJDBCLink link)
	{
		m_linkdata = new ConcurrentHashMap<String, Object>(link.getMappedData());
	}

	public BPJDBCSyncContextBase(Map<String, Object> linkdata)
	{
		m_linkdata = new ConcurrentHashMap<String, Object>(linkdata);
	}

	public void close()
	{
		Connection conn = m_conn;
		m_conn = null;
		if (conn != null)
			closeConnection(conn);
	}

	protected final static void closeConnection(Connection conn)
	{
		try
		{
			conn.close();
		}
		catch (SQLException e)
		{
		}
		finally
		{
		}
	}

	public void setCheckBeforeUse(boolean flag)
	{
		m_bcheck = flag;
	}

	protected boolean checkConnectionAtStart()
	{
		if (m_bcheck)
			return isValid();
		return true;
	}

	public boolean isValid()
	{
		boolean rc = false;
		try
		{
			rc = m_conn.isValid(10000);
		}
		catch (SQLException e)
		{
			return false;
		}
		return rc;
	}

	protected final static Connection createConnection(Map<String, Object> linkdata)
	{
		Map<String, Object> linkmap = linkdata;
		String url = (String) linkmap.get("url");
		String user = (String) linkmap.get("user");
		String pass = (String) linkmap.get("password");
		try
		{
			Connection conn = DriverManager.getConnection(url, user, pass);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException e)
		{
			Std.err(e);
			throw new RuntimeException(e);
		}
	}

	public boolean commit()
	{
		try
		{
			m_conn.commit();
			return true;
		}
		catch (SQLException e)
		{
			Std.err(e);
			return false;
		}
	}

	public boolean rollback()
	{
		try
		{
			m_conn.rollback();
			return true;
		}
		catch (SQLException e)
		{
			Std.err(e);
			return false;
		}
	}

	protected Connection getConnection()
	{
		Connection conn = m_conn;
		if (conn != null)
		{
			if (!checkConnectionAtStart())
			{
				closeConnection(conn);
				conn = null;
			}
		}
		if (conn == null)
		{
			conn = createConnection(m_linkdata);
			m_conn = conn;
		}
		return conn;
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

	public int execute(String sql, Object[] params)
	{
		return 0;
	}

	protected long writeQueryLog(String sql, Object[] params)
	{
		if (Std.getStdMode() == Std.STDMODE_DEBUG)
		{
			long tid = Thread.currentThread().getId();
			Std.debug(tid + ">" + sql);
			Std.debug(tid+"$[" + ObjUtil.joinArray(params, ",", null, false) + "]");
			return tid;
		}
		return 0;
	}

	protected void writeQueryLogEnd(long tid, long ct)
	{
		if (Std.getStdMode() == Std.STDMODE_DEBUG)
		{
			Std.debug(tid + "<" + (System.currentTimeMillis() - ct) + "ms");
		}
	}

	public BPXYData query(String sql, Object[] params)
	{
		Connection conn = getConnection();
		BPJDBCQueryResult result = null;
		if (conn != null)
		{
			Statement st = null;
			ResultSet rs = null;
			try
			{
				long t = writeQueryLog(sql, params);
				long ct = t != 0 ? System.currentTimeMillis() : 0;
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
				result.read(rs, 0, 0);
				writeQueryLogEnd(t, ct);
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
