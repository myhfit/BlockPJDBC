package bp.ext;

public class BPExtensionLoaderJDBC implements BPExtensionLoader
{
	public String getName()
	{
		return "JDBC";
	}

	public boolean isUI()
	{
		return false;
	}

	public String getUIType()
	{
		return null;
	}

	public String[] getParentExts()
	{
		return null;
	}

	public String[] getDependencies()
	{
		return null;
	}
}
