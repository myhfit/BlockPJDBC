package bp.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import bp.BPCLICore;
import bp.BPCore;
import bp.cli.console.BPConsoleHandlerDynamic;
import bp.data.BPCommand;
import bp.data.BPCommandResult;
import bp.data.BPXYData;
import bp.jdbc.BPJDBCContextBase;
import bp.jdbc.SQLCMDTYPE;
import bp.jdbc.BPJDBCContextBase.BPJDBCContextEnv;
import bp.jdbc.BPJDBCContextBase.JDBCThread;
import bp.project.BPResourceProject;
import bp.project.BPResourceProjectJDBC;
import bp.res.BPResource;
import bp.res.BPResourceJDBCLink;
import bp.util.LogicUtil;
import bp.util.ObjUtil;
import bp.util.Std;

public class BPCommandHandlerJDBC extends BPCommandHandlerBase implements BPCommandHandler
{
	public final static String CN_DB_USE = "db_use";
	public final static String CN_DB_LIST = "db_list";

	public BPCommandHandlerJDBC()
	{
		m_cmdnames = ObjUtil.makeList(CN_DB_USE, CN_DB_LIST);
	}

	public BPCommandResult call(BPCommand cmd)
	{
		String cmdname = cmd.name.toLowerCase();

		switch (cmdname)
		{
			case CN_DB_USE:
			{
				return useDB(getPSStringArr(cmd.ps));
			}
			case CN_DB_LIST:
			{
				return listDB(getPSStringArr(cmd.ps));
			}
		}
		return null;
	}

	protected BPCommandResult useDB(String[] ps)
	{
		if (ps.length > 1)
		{
			String prjname = ps[0];
			String dblinkname = ps[1];
			BPResourceProjectJDBC prj = (BPResourceProjectJDBC) BPCore.getProjectsContext().getProjectByName(prjname);
			List<BPResource> ress = prj.getProjectFunctionItems();
			BPResource jdbclink = null;
			for (BPResource res : ress)
			{
				if (res.getName().equals(dblinkname))
				{
					jdbclink = res;
					break;
				}
			}
			if (jdbclink != null)
			{
				BPDBCLIContext context = new BPDBCLIContext((BPResourceJDBCLink) jdbclink);
				context.start();
			}

		}
		return BPCommandResult.OK(null);
	}

	protected BPCommandResult listDB(String[] ps)
	{
		List<BPResourceProjectJDBC> prjs = new ArrayList<BPResourceProjectJDBC>();
		{
			BPResourceProject[] prjarr = BPCore.getProjectsContext().listProject();
			for (BPResourceProject prj : prjarr)
			{
				if (prj.getProjectTypeName() == BPResourceProjectJDBC.PRJTYPE_JDBC)
				{
					prjs.add((BPResourceProjectJDBC) prj);
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		for (BPResourceProjectJDBC prj : prjs)
		{
			List<BPResource> ress = prj.getProjectFunctionItems();
			for (BPResource res : ress)
			{
				if (sb.length() > 0)
					sb.append("\n");
				sb.append("[" + prj.getName() + "]" + res.getName());
			}
		}
		return BPCommandResult.OK(sb.toString());
	}

	public String getName()
	{
		return "JDBC";
	}

	protected static class BPDBCLIContext
	{
		private volatile BPJDBCContextBase m_context;
		private Function<String, BPCommandResult> m_cb;
		private String m_linkname;

		private final static String CN_CONNECT = "connect";
		private final static String CN_DISCONNECT = "disconnect";
		private final static String CN_COMMIT = "commit";
		private final static String CN_ROLLBACK = "rollback";

		public BPDBCLIContext(BPResourceJDBCLink jdbclink)
		{
			m_context = new BPJDBCContextBase(jdbclink);
			m_context.runSegment(() ->
			{
				JDBCThread t = (JDBCThread) Thread.currentThread();
				t.setEnv(BPJDBCContextEnv.KEY_QUERY_AUTOSTOP, "true");
				return null;
			});
			m_linkname = jdbclink.getName();
			m_cb = this::callCommand;
		}

		protected BPCommandResult callCommand(String cmd)
		{
			BPCommandResult rc = null;
			String cmdlower = cmd.toLowerCase();
			switch (cmdlower)
			{
				case CN_EXIT:
				{
					m_context.close();
					break;
				}
				case CN_HELP:
				{
					rc = BPCommandResult.OK(String.join("\n", CN_CONNECT, CN_DISCONNECT, CN_COMMIT, CN_ROLLBACK, CN_EXIT, "or direct input SQL to run"));
					break;
				}
				case CN_CONNECT:
				{
					rc = BPCommandResult.OK(LogicUtil.catchGet(() -> m_context.connect().toCompletableFuture().get(), false));
					break;
				}
				case CN_DISCONNECT:
				{
					rc = BPCommandResult.OK(LogicUtil.catchGet(() -> m_context.disconnect().toCompletableFuture().get(), false));
					break;
				}
				case CN_COMMIT:
				{
					rc = BPCommandResult.OK(LogicUtil.catchGet(() -> m_context.commit().toCompletableFuture().get(), false));
					break;
				}
				case CN_ROLLBACK:
				{
					rc = BPCommandResult.OK(LogicUtil.catchGet(() -> m_context.rollback().toCompletableFuture().get(), false));
					break;
				}
				default:
				{
					rc = BPCommandResult.OK(runSQL(cmd));
				}
			}
			return rc;
		}

		protected Object runSQL(String sql)
		{
			Object rc = null;
			sql = sql.trim();
			if (sql.length() > 0)
			{
				SQLCMDTYPE ct = SQLCMDTYPE.find(sql);
				if (ct == null)
				{
					String[] rct = { "Query", "Execute" };
					String s = Std.select(rct);
					if ("Execute".equals(s))
					{
						ct = SQLCMDTYPE.EXECUTE;
					}
					else if ("Query".equals(s))
					{
						ct = SQLCMDTYPE.QUERY;
					}
				}
				if (ct != null)
				{
					try
					{
						switch (ct)
						{
							case QUERY:
							{
								BPXYData xydata = m_context.startQuery(sql, new Object[] {}).toCompletableFuture().get();
								rc = xydata;
								break;
							}
							case EXECUTE:
							case CONTROL:
							case DEFINITION:
							{
								rc = m_context.execute(sql, new Object[] {}).toCompletableFuture().get();
								break;
							}
							default:
							{

							}
						}
					}
					catch (Exception e)
					{
						Std.err(e);
						return e.getMessage();
					}
				}
			}
			return rc;
		}

		public void start()
		{
			BPConsoleHandlerDynamic ch = new BPConsoleHandlerDynamic();
			ch.setHint("DB:" + m_linkname + ">");
			ch.setCommandHandler(m_cb);
			ch.setup(BPCLICore.lineLoop());
		}
	}
}