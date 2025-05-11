package bp.processor;

import java.util.concurrent.ExecutionException;

import bp.config.BPConfig;
import bp.config.BPSetting;
import bp.config.BPSettingBase;
import bp.format.BPFormatSQL;
import bp.format.BPFormatText;
import bp.jdbc.BPJDBCContextBase;
import bp.res.BPResource;
import bp.res.BPResourceHolder;
import bp.res.BPResourceHolder.BPResourceHolderW;
import bp.res.BPResourceIO;
import bp.res.BPResourceJDBCLink;
import bp.util.IOUtil;
import bp.util.TextUtil;

public abstract class BPResourceProcessorSQL implements BPResourceProcessor<BPResource, BPResource>
{
	public String getCategory()
	{
		return BPFormatSQL.FORMAT_SQL;
	}

	public boolean canInput(String format)
	{
		return format.equals(BPFormatSQL.FORMAT_SQL);
	}

	protected String readText(BPResource data, BPConfig config)
	{
		String txt = null;
		boolean readed = false;
		if (data instanceof BPResourceHolder)
		{
			Object obj = ((BPResourceHolder) data).getData();
			if (obj != null)
			{
				if (obj instanceof String)
				{
					txt = (String) obj;
					readed = true;
				}
			}
		}
		if (!readed && data.isIO())
		{
			BPResourceIO source = (BPResourceIO) data;
			txt = source.useInputStream(in -> TextUtil.toString(IOUtil.read(in), "utf-8"));
		}
		return txt;
	}

	public static class BPResourceProcessorSQLTestDB extends BPResourceProcessorSQL
	{
		public String getName()
		{
			return "SQL Processor - TestDB";
		}

		public String getUILabel()
		{
			return "TestDB";
		}

		public boolean canOutput(String format)
		{
			return format.equals(BPFormatText.FORMAT_TEXT);
		}

		public BPResource process(BPResource data, BPConfig config)
		{
			BPResourceJDBCLink jdbclink = config.get("jdbclink");

			String result;
			long ct = System.currentTimeMillis();
			try (BPJDBCContextBase context = new BPJDBCContextBase(jdbclink))
			{
				Boolean r = context.connect().toCompletableFuture().get();
				context.disconnect();
				result = r != null && r ? "Success:" : "Fail:";
				long ct2 = System.currentTimeMillis();
				result += (ct2 - ct) + "ms";
			}
			catch (InterruptedException | ExecutionException e)
			{
				result = e.toString();
			}
			BPResourceHolder.BPResourceHolderW rc = null;
			Object out = config.get("OUTPUT");
			if (out != null)
				rc = (BPResourceHolderW) out;
			else
				rc = new BPResourceHolder.BPResourceHolderW(null, null, BPFormatText.MIME_TEXT, null, null, true);
			rc.setData(result);
			return rc;
		}

		public BPSetting getSetting(BPConfig preset)
		{
			BPSettingBase rc = new BPSettingBase(preset);
			return rc;
		}
	}
}