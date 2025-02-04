package bp.transform;

import java.sql.Blob;
import java.sql.SQLException;

public class BPTransformerJDBC2ByteArray extends BPTransformerBase<Object>
{
	protected Object transform(Object t)
	{
		if (t == null)
			return null;
		if (t instanceof Blob)
		{
			Blob blob = (Blob) t;
			try
			{
				return blob.getBytes(0, (int) blob.length());
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
		return "JDBC to byte[]";
	}
}
