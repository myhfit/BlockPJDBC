package bp.transform;

import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Struct;

import bp.util.JSONUtil;

public class BPTransformerJDBC2Str extends BPTransformerBase<Object>
{
	protected Object transform(Object t)
	{
		if (t == null)
			return null;
		if (t instanceof Clob)
		{
			Clob clob = (Clob) t;
			try
			{
				return clob.getSubString(0, (int) clob.length());
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e);
			}
		}
		else if (t instanceof Struct)
		{
			Struct st = (Struct) t;
			Object[] obj = getStructObjs(st);
			if (obj != null)
				return JSONUtil.encode(obj);
			return null;
		}
		return null;
	}

	protected Object[] getStructObjs(Struct st)
	{
		try
		{
			Object[] rc = st.getAttributes();
			for (int i = 0; i < rc.length; i++)
			{
				Object o = rc[i];
				if (o != null && o instanceof Struct)
				{
					rc[i] = getStructObjs((Struct) rc[i]);
				}
			}
			return rc;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public String getInfo()
	{
		return "JDBC to Text";
	}
}
