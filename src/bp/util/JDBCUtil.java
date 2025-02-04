package bp.util;

import java.util.ServiceLoader;

import bp.jdbc.BPJDBCHelper;

public class JDBCUtil
{
	public final static BPJDBCHelper getHelper(String driver)
	{
		ServiceLoader<BPJDBCHelper> helpers = ClassUtil.getServices(BPJDBCHelper.class);
		for (BPJDBCHelper helper : helpers)
		{
			if (helper.checkDB(driver))
				return helper;
		}
		return null;
	}
}
