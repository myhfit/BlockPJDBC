package bp.jdbc;

public enum SQLCMDTYPE
{
	QUERY(1, new String[] { "SELECT", "PRAGMA" }), EXECUTE(1, new String[] { "INSERT", "UPDATE", "DELETE" }), COMMIT(2, new String[] { "COMMIT" }), ROLLBACK(2, new String[] { "ROLLBACK" }), SAVEPOINT(1, new String[] { "SAVEPOINT" }), CONTROL(1,
			new String[] { "GRANT", "REVOKE" }), DEFINITION(1, new String[] { "CREATE", "ALTER", "DROP" }),
	// CCL(1, new String[] {"DECLARE CURSOR","FETCH INTO",""})
	;

	private String[] m_checkstrs;
	private int m_checktype;

	private SQLCMDTYPE()
	{

	}

	private SQLCMDTYPE(int checktype, String[] checkstrs)
	{
		m_checktype = checktype;
		m_checkstrs = checkstrs;
	}

	public boolean match(String sql)
	{
		if (m_checktype == 1)
		{
			for (String checkstr : m_checkstrs)
			{
				if (sql.startsWith(checkstr))
				{
					return true;
				}
			}
			return false;
		}
		else if (m_checktype == 2)
		{
			for (String checkstr : m_checkstrs)
			{
				if (sql.equals(checkstr))
				{
					return true;
				}
			}
			return false;
		}
		return true;
	}

	public static SQLCMDTYPE find(String sql)
	{
		String usql = sql.trim().toUpperCase();
		for (SQLCMDTYPE ct : SQLCMDTYPE.values())
		{
			if (ct.match(usql))
				return ct;
		}
		return null;
	}
}
