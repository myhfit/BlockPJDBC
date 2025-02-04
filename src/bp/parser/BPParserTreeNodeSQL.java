package bp.parser;

import java.text.ParseException;

public class BPParserTreeNodeSQL extends BPParserTreeNodeBase<BPParserTreeNodeSQL>
{
	public SQLKeyword key;
	
	public int parse(String str,int start,int end) throws ParseException
	{
		int i=start;
		for(;i<=end;i++)
		{
//			char c=str.charAt(i);
		}
		return i;
	}
	
	public static enum SQLKeyword
	{
		SELECT,UPDATE,WHERE,FROM,DELETE,INSERTINTO,OTHER;
	}
}
