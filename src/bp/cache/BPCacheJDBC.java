package bp.cache;

import bp.res.BPResourceDBItem.BPResourceDBTable;
import bp.res.BPResourceJDBCLink;
import bp.res.BPResourceJDBCLink.DBStruct;

public interface BPCacheJDBC extends BPCache
{
	DBStruct getDBStruct(BPResourceJDBCLink link);

	void addCacheTask(BPResourceJDBCLink link);

	void addCacheTask(BPResourceDBTable table);
}
