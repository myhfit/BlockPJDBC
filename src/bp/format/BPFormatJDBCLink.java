package bp.format;

public class BPFormatJDBCLink implements BPFormat
{
	public final static String FORMAT_JDBCLINK = "JDBC Link";

	public String getName()
	{
		return FORMAT_JDBCLINK;
	}

	public String[] getExts()
	{
		return new String[] { ".bpjdbc" };
	}

}
