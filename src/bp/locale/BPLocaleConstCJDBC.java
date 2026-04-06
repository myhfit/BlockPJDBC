package bp.locale;

//Computer JDBC Dict
public enum BPLocaleConstCJDBC implements BPLocaleConstSimple
{
	Driver,
	URL,
	DB_Categories,
	TestDB,
	;

	public final static String PACK_COMPUTER_JDBC = "c_jdbc";

	public String getPackName()
	{
		return PACK_COMPUTER_JDBC;
	}

	public boolean needNormalizeCase()
	{
		return false;
	}
}
