package bp.jdbc;

public interface BPJDBCHelper
{
	String getDBName();

	String getHelperName();

	boolean checkDB(String driver);

	boolean checkFeature(String actionname);

	<T> T doAction(String actionname, Object... params);

	public final static String ACT_GETDDL_CREATETABLE = "DDL_CREATETABLE";
}
