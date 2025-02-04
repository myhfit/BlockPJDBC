package bp.parser;

import java.text.ParseException;

public class BPParserSQL implements BPParser<String, BPParserTreeNodeSQL>
{
	public BPParserTreeNodeSQL parse(String source)
	{
		BPParserTreeNodeSQL root = new BPParserTreeNodeSQL();

		try
		{
			root.parse(source, 0, source.length() - 1);
			return root;
		}
		catch (ParseException pe)
		{
			return null;
		}
	}

}
