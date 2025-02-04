package bp.jdbc;

import java.util.List;
import java.util.concurrent.ExecutionException;

import bp.data.BPXData;
import bp.data.BPXYData;
import bp.util.Std;

public class BPJDBCHelperOracle implements BPJDBCHelper
{
	public String getDBName()
	{
		return "Oracle";
	}

	public String getHelperName()
	{
		return "Oracle Helper";
	}

	public boolean checkDB(String driver)
	{
		if ("oracle.jdbc.OracleDriver".equals(driver))
			return true;
		if ("oracle.jdbc.driver.OracleDriver".equals(driver))
			return true;
		return false;
	}

	public boolean checkFeature(String actionname)
	{
		switch (actionname)
		{
			case ACT_GETDDL_CREATETABLE:
				return true;
		}
		return false;
	}

	public String getCreateTableDDL(BPJDBCContext context, String tablename, String schemaname)
	{
		try
		{
			BPXYData r = context.query("SELECT DBMS_METADATA.GET_DDL('TABLE',?,?) FROM DUAL", new Object[] { tablename, schemaname }).toCompletableFuture().get();
			if (r != null)
			{
				List<BPXData> datas = r.getDatas();
				if (datas.size() > 0)
				{
					return (String) datas.get(0).getColValue(0);
				}
			}
		}
		catch (InterruptedException | ExecutionException e)
		{
			Std.err(e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> T doAction(String actionname, Object... params)
	{
		switch (actionname)
		{
			case ACT_GETDDL_CREATETABLE:
				return (T) getCreateTableDDL((BPJDBCContext) params[0], (String) params[1], (String) params[2]);
		}
		return null;
	}

}
