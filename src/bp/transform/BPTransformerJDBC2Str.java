package bp.transform;

import java.sql.Clob;
import java.sql.SQLException;

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
		return null;
	}

	public String getInfo()
	{
		return "JDBC to Text";
	}
}
