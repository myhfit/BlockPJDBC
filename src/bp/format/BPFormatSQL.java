package bp.format;

public class BPFormatSQL implements BPFormat
{
	public final static String FORMAT_SQL = "SQL";

	public String getName()
	{
		return FORMAT_SQL;
	}

	public String[] getExts()
	{
		return new String[] { ".sql", "text/x-sql" };
	}
}
