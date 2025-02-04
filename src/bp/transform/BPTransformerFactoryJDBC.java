package bp.transform;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Struct;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

public class BPTransformerFactoryJDBC implements BPTransformerFactory
{
	public String getName()
	{
		return "JDBC";
	}

	public boolean checkData(Object source)
	{
		if (source instanceof Blob || source instanceof Clob || source instanceof Struct)
			return true;
		return false;
	}

	public Collection<String> getFunctionTypes()
	{
		return new CopyOnWriteArrayList<String>(new String[] { TF_TOSTRING, TF_TOBYTEARRAY });
	}

	public BPTransformer<?> createTransformer(String func)
	{
		if (TF_TOSTRING.equals(func))
			return new BPTransformerJDBC2Str();
		else if (TF_TOBYTEARRAY.equals(func))
			return new BPTransformerJDBC2ByteArray();
		return null;
	}

}
