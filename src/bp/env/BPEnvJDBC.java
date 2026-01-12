package bp.env;

import bp.env.BPEnvDynamic.BPEnvDynamicSimple;

public class BPEnvJDBC extends BPEnvDynamicSimple
{
	public BPEnvJDBC(String name)
	{
		super(name);
		m_rawkeys.add("driver");
		m_rawkeys.add("url");
		m_rawkeys.add("user");
		m_rawkeys.add("password");
	}
}
