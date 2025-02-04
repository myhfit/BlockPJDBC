package bp.jdbc;

public class BPJDBCHelperPostgreSQL implements BPJDBCHelper
{
	public String getDBName()
	{
		return "PostgreSQL";
	}

	public String getHelperName()
	{
		return "PostgreSQL Helper";
	}

	public boolean checkDB(String driver)
	{
		if ("org.postgresql.Driver".equals(driver))
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

	public <T> T doAction(String actionname, Object... params)
	{
		return null;
	}

}
