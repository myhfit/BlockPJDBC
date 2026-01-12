package bp.datasource;

import java.io.IOException;
import java.util.function.Function;

import bp.data.BPDataSource;
import bp.jdbc.BPJDBCSyncContext;

public interface BPDataSourceJDBC extends BPDataSource
{
	default void close() throws IOException
	{
	}

	default BPDataSourceType getDSType()
	{
		return BPDataSourceType.JDBC;
	}

	<R> R use(Function<BPJDBCSyncContext, R> seg);
}