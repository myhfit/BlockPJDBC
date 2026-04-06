package bp.ext;

import bp.BPCore;
import bp.BPCore.BPPlatform;
import bp.context.BPFileContext;
import bp.core.BPCommandHandlerJDBC;
import bp.locale.BPLocaleConstCJDBC;
import bp.locale.BPLocaleHelperDirect;
import bp.locale.BPLocaleHelpers;

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
		BPLocaleHelpers.registerHelper(BPLocaleHelperDirect.createHelper(BPLocaleConstCJDBC.class, null, BPLocaleConstCJDBC.PACK_COMPUTER_JDBC, null));
	}

	public String[] getDependencies()
	{
		return null;
	}
}
