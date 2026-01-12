package bp.format;

public class BPFormatSQL implements BPFormat
{
	public final static String FORMAT_SQL = "SQL";
	public final static String MIME_SQL = "application/sql";

	public String getName()
	{
		return FORMAT_SQL;
	}

	public String[] getExts()
	{
		return new String[] { ".sql", MIME_SQL };
	}

	public String getMIME()
	{
		return MIME_SQL;
	}
}
