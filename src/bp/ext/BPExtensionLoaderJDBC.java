package bp.ext;

import bp.BPCore;
import bp.BPCore.BPPlatform;
import bp.context.BPFileContext;
import bp.core.BPCommandHandlerJDBC;

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

	public void install(BPFileContext context)
	{
		if (BPCore.getPlatform() == BPPlatform.CLI)
			BPCore.addCommandHandler(new BPCommandHandlerJDBC());
	}

	public String[] getDependencies()
	{
		return null;
	}
}
