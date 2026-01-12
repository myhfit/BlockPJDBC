package bp.jdbc;

import bp.data.BPXYData;

public interface BPJDBCSyncContext
{
	boolean commit();

	boolean rollback();

	int execute(String sql, Object[] params);

	BPXYData query(String sql, Object[] params);

	boolean isValid();

	void close();
}
