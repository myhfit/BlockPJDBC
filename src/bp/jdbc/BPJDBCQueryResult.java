package bp.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import bp.data.BPXData;
import bp.data.BPXYDDataBase;
import bp.util.Std;

public class BPJDBCQueryResult extends BPXYDDataBase
{
	protected final static int COLF_BOOLEAN = 1;
	protected final static int COLF_STRING = 2;
	protected final static int COLF_SHORT = 3;
	protected final static int COLF_INT = 4;
	protected final static int COLF_FLOAT = 5;
	protected final static int COLF_LONG = 6;
	protected final static int COLF_DOUBLE = 7;
	protected final static int COLF_BIGDECIMAL = 8;
	protected final static int COLF_DATE = 9;
	protected final static int COLF_TIME = 10;
	protected final static int COLF_BYTES = 11;

	public final static BPJDBCQueryResult createByMetaData(ResultSetMetaData metadata)
	{
		BPJDBCQueryResult rc = new BPJDBCQueryResult();
		rc.initColumns(metadata);
		return rc;
	}

	protected Class<?> mapColClass(int st)
	{
		switch (st)
		{
			case Types.BOOLEAN:
			case Types.BIT:
				return Boolean.class;
			case Types.CHAR:
			case Types.LONGNVARCHAR:
			case Types.VARCHAR:
				return String.class;
			case Types.TINYINT:
				return Short.class;
			case Types.SMALLINT:
				return Short.class;
			case Types.INTEGER:
				return Integer.class;
			case Types.REAL:
				return Float.class;
			case Types.BIGINT:
				return Long.class;
			case Types.DOUBLE:
			case Types.FLOAT:
				return Double.class;
			case Types.DECIMAL:
			case Types.NUMERIC:
				return BigDecimal.class;
			case Types.DATE:
				return java.util.Date.class;
			case Types.TIME:
			case Types.TIMESTAMP:
				return java.util.Date.class;
			case Types.CLOB:
				return String.class;
			case Types.BLOB:
				return byte[].class;
		}
		return Object.class;
	}

	protected boolean initColumns(ResultSetMetaData metadata)
	{
		Class<?>[] ccs = null;
		String[] cns = null;
		String[] cls = null;
		try
		{
			int colcount = metadata.getColumnCount();
			ccs = new Class<?>[colcount];
			cns = new String[colcount];
			cls = new String[colcount];
			for (int i = 0; i < colcount; i++)
			{
				cns[i] = metadata.getColumnName(i + 1);
				cls[i] = metadata.getColumnLabel(i + 1);
				ccs[i] = mapColClass(metadata.getColumnType(i + 1));
			}
		}
		catch (SQLException e)
		{
			Std.err(e);
			return false;
		}
		m_cns = cns;
		m_ccs = ccs;
		m_cls = cls;
		return true;
	}

	public boolean read(ResultSet rs, int stopcount, int pagecount)
	{
		return read(rs, stopcount, pagecount, null);
	}

	public boolean read(ResultSet rs, int stopcount, int pagecount, Supplier<Boolean> checkstopfunc)
	{
		int[] ffs = genColFF();
		int c = m_ccs.length;
		boolean isend = true;
		try
		{
			List<BPXData> datas = new ArrayList<BPXData>();
			int stoprowcount = 0;
			int pagerowcount = 0;
			List<BPXData> pagedata = ((pagecount > 0) ? new ArrayList<BPXData>() : null);
			while (rs.next())
			{
				Object[] row = new Object[c];
				for (int i = 0; i < c; i++)
				{
					Object oval;
					int i1 = i + 1;
					switch (ffs[i])
					{
						case COLF_BOOLEAN:
							oval = rs.getBoolean(i1);
							break;
						case COLF_STRING:
							oval = rs.getString(i1);
							break;
						case COLF_SHORT:
							oval = rs.getShort(i1);
							break;
						case COLF_INT:
							oval = rs.getInt(i1);
							break;
						case COLF_FLOAT:
							oval = rs.getFloat(i1);
							break;
						case COLF_LONG:
							oval = rs.getLong(i1);
							break;
						case COLF_DOUBLE:
							oval = rs.getDouble(i1);
							break;
						case COLF_BIGDECIMAL:
							oval = rs.getBigDecimal(i1);
							break;
						case COLF_DATE:
						{
							Timestamp t = rs.getTimestamp(i1);
							oval = ((t == null) ? null : new java.util.Date(t.getTime()));
							break;
						}
						case COLF_TIME:
						{
							Timestamp t = rs.getTimestamp(i1);
							oval = ((t == null) ? null : new java.util.Date(t.getTime()));
							break;
						}
						case COLF_BYTES:
						{
							oval = rs.getBytes(i1);
						}
						default:
						{
							oval = rs.getObject(i1);
						}
					}
					row[i] = oval;
				}
				BPXData.BPXDataList record = new BPXData.BPXDataList(row);
				datas.add(record);
				stoprowcount++;
				pagerowcount++;
				if (pagecount > 0)
				{
					pagedata.add(record);
					if (pagerowcount >= pagecount)
					{
						appendPage(pagedata);
						pagedata = new ArrayList<>();
						pagerowcount = 0;
					}
				}
				if (stopcount > 0 && stoprowcount >= stopcount)
				{
					if (checkstopfunc == null || checkstopfunc.get())
					{
						isend = false;
						break;
					}
					else
					{
						stoprowcount = 0;
					}
				}
				else
				{
					if (checkstopfunc != null && checkstopfunc.get())
					{
						isend = false;
						break;
					}
				}
			}
			if (pagedata != null && pagedata.size() > 0)
				appendPage(pagedata);
			m_datas = new CopyOnWriteArrayList<BPXData>(datas);
		}
		catch (SQLException e)
		{
			Std.err(e);
		}
		return isend;
	}

	protected int[] genColFF()
	{
		int colcount = m_ccs.length;
		int[] colff = new int[colcount];
		for (int i = 0; i < colcount; i++)
		{
			Class<?> cls = m_ccs[i];
			if (cls == Boolean.class)
			{
				colff[i] = COLF_BOOLEAN;
			}
			else if (cls == String.class)
			{
				colff[i] = COLF_STRING;
			}
			else if (cls == Short.class)
			{
				colff[i] = COLF_SHORT;
			}
			else if (cls == Integer.class)
			{
				colff[i] = COLF_INT;
			}
			else if (cls == Float.class)
			{
				colff[i] = COLF_FLOAT;
			}
			else if (cls == Long.class)
			{
				colff[i] = COLF_LONG;
			}
			else if (cls == Double.class)
			{
				colff[i] = COLF_DOUBLE;
			}
			else if (cls == BigDecimal.class)
			{
				colff[i] = COLF_BIGDECIMAL;
			}
			else if (cls == java.util.Date.class)
			{
				colff[i] = COLF_DATE;
			}
			else if (cls == Time.class)
			{
				colff[i] = COLF_TIME;
			}
			else if (cls == byte[].class)
			{
				colff[i] = COLF_BYTES;
			}
		}
		return colff;
	}
}